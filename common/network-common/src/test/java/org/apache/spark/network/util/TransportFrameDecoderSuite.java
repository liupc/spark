/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Joiner;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.RecvByteBufAllocator;
import org.apache.commons.lang3.SystemUtils;
import org.junit.AfterClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TransportFrameDecoderSuite {

  private static Random RND = new Random();

  @AfterClass
  public static void cleanup() {
    RND = null;
  }

  @Test
  public void testFrameDecoding() throws Exception {
    TransportFrameDecoder decoder = new TransportFrameDecoder();
    ChannelHandlerContext ctx = mockChannelHandlerContext();
    ByteBuf data = createAndFeedFrames(100, decoder, ctx);
    verifyAndCloseDecoder(decoder, ctx, data);
  }

  @Test
  public void testConsolidationForDecodingNonFullyWrittenByteBuf() {
    TransportFrameDecoder decoder = new TransportFrameDecoder();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    List<ByteBuf> retained = new ArrayList<>();
    when(ctx.fireChannelRead(any())).thenAnswer(in -> {
      ByteBuf buf = (ByteBuf) in.getArguments()[0];
      retained.add(buf);
      return null;
    });
    ByteBuf data1 = Unpooled.buffer(1024 * 1024);
    data1.writeLong(1024 * 1024 + 8);
    data1.writeByte(127);
    ByteBuf data2 = Unpooled.buffer(1024 * 1024);
    for (int i = 0; i < 1024 * 1024 - 1; i++) {
      data2.writeByte(128);
    }
    int orignalCapacity = data1.capacity() + data2.capacity();
    try {
      decoder.channelRead(ctx, data1);
      decoder.channelRead(ctx, data2);
      assertEquals(1, retained.size());
      assert(retained.get(0).capacity() < orignalCapacity);
    } catch (Exception e) {
      release(data1);
      release(data2);
    }
  }

  @Test
  public void getJVMOSInfo() {
    String vmName = System.getProperty("java.vm.name");
    String runtimeVersion = System.getProperty("java.runtime.version");
    String osName = System.getProperty("os.name");
    String osVersion = System.getProperty("os.version");
    System.out.println(String.format("%s %s on %s %s", vmName, runtimeVersion, osName, osVersion));
  }

  // Processor info
  // cat /proc/cpuinfo|grep "model name"

  private class Metrics {
    long totalTime;
  }
  private CompositeByteBuf createCompositeBuf(
      ByteBufAllocator alloc, int numComponents, int size, int writtenBytes, Metrics metrics, int plan) {
    long lastConsolidateWriterIndex = 0;
    CompositeByteBuf compositeByteBuf = alloc.compositeBuffer(Integer.MAX_VALUE);
    for (int i =0; i< numComponents; i++) {
      ByteBuf buf = alloc.ioBuffer(size);
      buf.writeBytes(ByteBuffer.allocate(writtenBytes));
      compositeByteBuf.addComponent(buf).writerIndex(compositeByteBuf.writerIndex() + buf.readableBytes());
      if (plan == 2) {
        // plan 2
        if (i % 16 == 0) {
          long start = System.currentTimeMillis();
          compositeByteBuf.consolidate();
          long cost = System.currentTimeMillis() - start;
          metrics.totalTime += cost;
        }
      }
      if (plan == 3) {
        // plan 3
        if (compositeByteBuf.writerIndex() - lastConsolidateWriterIndex >= 300 * 1024 * 1024) {
          long start = System.currentTimeMillis();
          compositeByteBuf.consolidate();
          long cost = System.currentTimeMillis() - start;
          metrics.totalTime += cost;
          lastConsolidateWriterIndex = compositeByteBuf.writerIndex();
        }
      }
    }
    if (plan == 1) {
      // plan 1
      long start = System.currentTimeMillis();
      compositeByteBuf.consolidate();
      long cost = System.currentTimeMillis() - start;
      metrics.totalTime += cost;
    }

    return compositeByteBuf;
  }

  // 1. only consolidate the final compositebuf
  // 2. consolidate the compositebuf once the maxComponents is exceeded, default to 16
  // 3. consolidate the compositebuf when the accumulated written bytes exceeds 0.3 * memoryOverhead( 300M for 1GB)
  // NOTE: assume sixty percent of memoryOverhead can be used in shuffle(when in shuffle phase, the memoryOverhead are mainly used here),
  // in the worst case, the writerIndex is equal to the capacity, then we must reserve half of this memory for consolidation, thus we get 0.3.
  // this is a conservative esitmate, for most cases, we can make the threshold higher, but we just keep it as unchanged for better safety.
  private void testConsolidateWithLoop(String testName, ByteBufAllocator alloc, int numComponents, int size, int util, int loopCount) {
    int writtenBytes = (int)((double)size * util / 100);
    Metrics metrics = new Metrics();
    for (int i = 0; i < loopCount; i++) {
      // Test consolidate with different plan - 1 for plan1, 2 for plan2, 3 for plan3
      CompositeByteBuf buf = createCompositeBuf(alloc, numComponents, size, writtenBytes, metrics, 2);
      buf.release();
    }
    System.out.println("[" + testName + "]");
    System.out.println("Allocating " + writtenBytes + " bytes");
    System.out.println("Time cost with " + loopCount + " loop for consolidating: " + metrics.totalTime + " millis");
    System.out.println();
  }
  ///////////////////////////////////////////////////////////
  //  Java HotSpot(TM) 64-Bit Server VM 1.8.0_111-b14 on Linux 4.15.0-43-generic
  //  Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz

  //  --- Test Reports for plan 1 ------
  //  [test consolidate 1000 buffers each with 1m, 50% used for 1 loop]
  //  Allocating 524288 bytes
  //  Time cost with 1 loop for consolidating: 207 millis
  //
  //  [test consolidate 1000 buffers each with 1m, 100% used for 1 loop]
  //  Allocating 1048576 bytes
  //  Time cost with 1 loop for consolidating: 340 millis
  //
  //  [test consolidate 1000 buffers each with 1m, 50% used for 10 loop]
  //  Allocating 524288 bytes
  //  Time cost with 10 loop for consolidating: 1635 millis
  //
  //  [test consolidate 1000 buffers each with 1m, 100% used for 10 loop]
  //  Allocating 1048576 bytes
  //  Time cost with 10 loop for consolidating: 3172 millis
  //
  //  [test consolidate 1000 buffers each with 1m, 50% used for 50 loop]
  //  Allocating 524288 bytes
  //  Time cost with 50 loop for consolidating: 8017 millis
  //
  //  [test consolidate 1000 buffers each with 1m, 100% used for 50 loop]
  //  Allocating 1048576 bytes
  //  Time cost with 50 loop for consolidating: 15497 millis

  //  [test consolidate 100 buffers each with 10m, 50% used for 1 loop]
  //  Allocating 5242880 bytes
  //  Time cost with 1 loop for consolidating: 223 millis
  //
  //  [test consolidate 100 buffers each with 10m, 100% used for 1 loop]
  //  Allocating 10485760 bytes
  //  Time cost with 1 loop for consolidating: 451 millis
  //
  //  [test consolidate 100 buffers each with 10m, 50% used for 10 loop]
  //  Allocating 5242880 bytes
  //  Time cost with 10 loop for consolidating: 1924 millis
  //
  //  [test consolidate 100 buffers each with 10m, 100% used for 10 loop]
  //  Allocating 10485760 bytes
  //  Time cost with 10 loop for consolidating: 3787 millis
  //
  //  [test consolidate 100 buffers each with 10m, 50% used for 50 loop]
  //  Allocating 5242880 bytes
  //  Time cost with 50 loop for consolidating: 8870 millis
  //
  //  [test consolidate 100 buffers each with 10m, 100% used for 50 loop]
  //  Allocating 10485760 bytes
  //  Time cost with 50 loop for consolidating: 17472 millis
  //
  //  [test consolidate 20 buffers each with 50m, 50% used for 1 loop]
  //  Allocating 26214400 bytes
  //  Time cost with 1 loop for consolidating: 184 millis
  //
  //  [test consolidate 20 buffers each with 50m, 100% used for 1 loop]
  //  Allocating 52428800 bytes
  //  Time cost with 1 loop for consolidating: 367 millis
  //
  //  [test consolidate 20 buffers each with 50m, 50% used for 10 loop]
  //  Allocating 26214400 bytes
  //  Time cost with 10 loop for consolidating: 1847 millis
  //
  //  [test consolidate 20 buffers each with 50m, 100% used for 10 loop]
  //  Allocating 52428800 bytes
  //  Time cost with 10 loop for consolidating: 3638 millis
  //
  //  [test consolidate 20 buffers each with 50m, 50% used for 50 loop]
  //  Allocating 26214400 bytes
  //  Time cost with 50 loop for consolidating: 9126 millis
  //
  //  [test consolidate 20 buffers each with 50m, 100% used for 50 loop]
  //  Allocating 52428800 bytes
  //  Time cost with 50 loop for consolidating: 19391 millis
  //
  //  [test consolidate 10 buffers each with 100m, 50% used for 1 loop]
  //  Allocating 52428800 bytes
  //  Time cost with 1 loop for consolidating: 211 millis
  //
  //  [test consolidate 10 buffers each with 100m, 100% used for 1 loop]
  //  Allocating 104857600 bytes
  //  Time cost with 1 loop for consolidating: 400 millis
  //
  //  [test consolidate 10 buffers each with 100m, 50% used for 10 loop]
  //  Allocating 52428800 bytes
  //  Time cost with 10 loop for consolidating: 1954 millis
  //
  //  [test consolidate 10 buffers each with 100m, 100% used for 10 loop]
  //  Allocating 104857600 bytes
  //  Time cost with 10 loop for consolidating: 3846 millis
  //
  //  [test consolidate 10 buffers each with 100m, 50% used for 50 loop]
  //  Allocating 52428800 bytes
  //  Time cost with 50 loop for consolidating: 9747 millis
  //
  //  [test consolidate 10 buffers each with 100m, 100% used for 50 loop]
  //  Allocating 104857600 bytes
  //  Time cost with 50 loop for consolidating: 19542 millis

  //  --- Test Reports for plan 2 ------
  //
  // [test consolidate 1000 buffers each with 1m, 50% used for 1 loop]
  // Allocating 524288 bytes
  // Time cost with 1 loop for consolidating: 5338 millis
  //
  // [test consolidate 1000 buffers each with 1m, 100% used for 1 loop]
  // Allocating 1048576 bytes
  // Time cost with 1 loop for consolidating: 10220 millis
  //
  // [test consolidate 1000 buffers each with 1m, 50% used for 10 loop]
  // Allocating 524288 bytes
  // Time cost with 10 loop for consolidating: 49249 millis
  //
  // [test consolidate 1000 buffers each with 1m, 100% used for 10 loop]
  // Allocating 1048576 bytes
  // Time cost with 10 loop for consolidating: 99247 millis
  //
  // [test consolidate 1000 buffers each with 1m, 50% used for 50 loop]
  // Allocating 524288 bytes
  // Time cost with 50 loop for consolidating: 249160 millis
  // ...... too slow
  //
  //  --- Test Reports for plan 3 ---
  //  [test consolidate 1000 buffers each with 1m, 50% used for 1 loop]
  //  Allocating 524288 bytes
  //  Time cost with 1 loop for consolidating: 116 millis
  //
  //  [test consolidate 1000 buffers each with 1m, 100% used for 1 loop]
  //  Allocating 1048576 bytes
  //  Time cost with 1 loop for consolidating: 635 millis
  //
  //  [test consolidate 1000 buffers each with 1m, 50% used for 10 loop]
  //  Allocating 524288 bytes
  //  Time cost with 10 loop for consolidating: 1003 millis
  //
  //  [test consolidate 1000 buffers each with 1m, 100% used for 10 loop]
  //  Allocating 1048576 bytes
  //  Time cost with 10 loop for consolidating: 5702 millis
  //
  //  [test consolidate 1000 buffers each with 1m, 50% used for 50 loop]
  //  Allocating 524288 bytes
  //  Time cost with 50 loop for consolidating: 4799 millis
  //
  //  [test consolidate 1000 buffers each with 1m, 100% used for 50 loop]
  //  Allocating 1048576 bytes
  //  Time cost with 50 loop for consolidating: 28440 millis
  //
  //  [test consolidate 100 buffers each with 10m, 50% used for 1 loop]
  //  Allocating 5242880 bytes
  //  Time cost with 1 loop for consolidating: 96 millis
  //
  //  [test consolidate 100 buffers each with 10m, 100% used for 1 loop]
  //  Allocating 10485760 bytes
  //  Time cost with 1 loop for consolidating: 571 millis
  //
  //  [test consolidate 100 buffers each with 10m, 50% used for 10 loop]
  //  Allocating 5242880 bytes
  //  Time cost with 10 loop for consolidating: 940 millis
  //
  //  [test consolidate 100 buffers each with 10m, 100% used for 10 loop]
  //  Allocating 10485760 bytes
  //  Time cost with 10 loop for consolidating: 5727 millis
  //
  //  [test consolidate 100 buffers each with 10m, 50% used for 50 loop]
  //  Allocating 5242880 bytes
  //  Time cost with 50 loop for consolidating: 4739 millis
  //
  //  [test consolidate 100 buffers each with 10m, 100% used for 50 loop]
  //  Allocating 10485760 bytes
  //  Time cost with 50 loop for consolidating: 28356 millis
  //
  //  [test consolidate 20 buffers each with 50m, 50% used for 1 loop]
  //  Allocating 26214400 bytes
  //  Time cost with 1 loop for consolidating: 96 millis
  //
  //  [test consolidate 20 buffers each with 50m, 100% used for 1 loop]
  //  Allocating 52428800 bytes
  //  Time cost with 1 loop for consolidating: 577 millis
  //
  //  [test consolidate 20 buffers each with 50m, 50% used for 10 loop]
  //  Allocating 26214400 bytes
  //  Time cost with 10 loop for consolidating: 966 millis
  //
  //  [test consolidate 20 buffers each with 50m, 100% used for 10 loop]
  //  Allocating 52428800 bytes
  //  Time cost with 10 loop for consolidating: 5731 millis
  //
  //  [test consolidate 20 buffers each with 50m, 50% used for 50 loop]
  //  Allocating 26214400 bytes
  //  Time cost with 50 loop for consolidating: 4826 millis
  //
  //  [test consolidate 20 buffers each with 50m, 100% used for 50 loop]
  //  Allocating 52428800 bytes
  //  Time cost with 50 loop for consolidating: 28734 millis
  //
  //  [test consolidate 10 buffers each with 100m, 50% used for 1 loop]
  //  Allocating 52428800 bytes
  //  Time cost with 1 loop for consolidating: 98 millis
  //
  //  [test consolidate 10 buffers each with 100m, 100% used for 1 loop]
  //  Allocating 104857600 bytes
  //  Time cost with 1 loop for consolidating: 576 millis
  //
  //  [test consolidate 10 buffers each with 100m, 50% used for 10 loop]
  //  Allocating 52428800 bytes
  //  Time cost with 10 loop for consolidating: 965 millis
  //
  //  [test consolidate 10 buffers each with 100m, 100% used for 10 loop]
  //  Allocating 104857600 bytes
  //  Time cost with 10 loop for consolidating: 6122 millis
  //
  //  [test consolidate 10 buffers each with 100m, 50% used for 50 loop]
  //  Allocating 52428800 bytes
  //  Time cost with 50 loop for consolidating: 5228 millis
  //
  //  [test consolidate 10 buffers each with 100m, 100% used for 50 loop]
  //  Allocating 104857600 bytes
  //  Time cost with 50 loop for consolidating: 30797 millis
  ////////////////////////////////////////////////////////////

  @Test
  public void benchmarkForConsolidation() throws Exception {
    PooledByteBufAllocator alloc = new PooledByteBufAllocator(true);

    testConsolidateWithLoop("test consolidate 1000 buffers each with 1m, 50% used for 1 loop",
        alloc, 1000, 1024 * 1024, 50, 1);

    testConsolidateWithLoop("test consolidate 1000 buffers each with 1m, 100% used for 1 loop",
        alloc, 1000, 1024 * 1024, 100, 1);
    testConsolidateWithLoop("test consolidate 1000 buffers each with 1m, 50% used for 10 loop",
        alloc, 1000, 1024 * 1024, 50, 10);

    testConsolidateWithLoop("test consolidate 1000 buffers each with 1m, 100% used for 10 loop",
        alloc, 1000, 1024 * 1024, 100, 10);

    testConsolidateWithLoop("test consolidate 1000 buffers each with 1m, 50% used for 50 loop",
        alloc, 1000, 1024 * 1024, 50, 50);

    testConsolidateWithLoop("test consolidate 1000 buffers each with 1m, 100% used for 50 loop",
        alloc, 1000, 1024 * 1024, 100, 50);
    /////////

    testConsolidateWithLoop("test consolidate 100 buffers each with 10m, 50% used for 1 loop",
        alloc, 100, 1024 * 1024 * 10, 50, 1);

    testConsolidateWithLoop("test consolidate 100 buffers each with 10m, 100% used for 1 loop",
        alloc, 100, 1024 * 1024 * 10, 100, 1);


    testConsolidateWithLoop("test consolidate 100 buffers each with 10m, 50% used for 10 loop",
        alloc, 100, 1024 * 1024 * 10, 50, 10);

    testConsolidateWithLoop("test consolidate 100 buffers each with 10m, 100% used for 10 loop",
        alloc, 100, 1024 * 1024 * 10, 100, 10);

    testConsolidateWithLoop("test consolidate 100 buffers each with 10m, 50% used for 50 loop",
        alloc, 100, 1024 * 1024 * 10, 50, 50);

    testConsolidateWithLoop("test consolidate 100 buffers each with 10m, 100% used for 50 loop",
        alloc, 100, 1024 * 1024 * 10, 100, 50);

    /////////
    testConsolidateWithLoop("test consolidate 20 buffers each with 50m, 50% used for 1 loop",
        alloc, 20, 1024 * 1024 * 50, 50, 1);
    testConsolidateWithLoop("test consolidate 20 buffers each with 50m, 100% used for 1 loop",
        alloc, 20, 1024 * 1024 * 50, 100, 1);
    testConsolidateWithLoop("test consolidate 20 buffers each with 50m, 50% used for 10 loop",
        alloc, 20, 1024 * 1024 * 50, 50, 10);
    testConsolidateWithLoop("test consolidate 20 buffers each with 50m, 100% used for 10 loop",
        alloc, 20, 1024 * 1024 * 50, 100, 10);
    testConsolidateWithLoop("test consolidate 20 buffers each with 50m, 50% used for 50 loop",
        alloc, 20, 1024 * 1024 * 50, 50, 50);
    testConsolidateWithLoop("test consolidate 20 buffers each with 50m, 100% used for 50 loop",
        alloc, 20, 1024 * 1024 * 50, 100, 50);

    //////
    testConsolidateWithLoop("test consolidate 10 buffers each with 100m, 50% used for 1 loop",
        alloc, 10, 1024 * 1024 * 100, 50, 1);
    testConsolidateWithLoop("test consolidate 10 buffers each with 100m, 100% used for 1 loop",
        alloc, 10, 1024 * 1024 * 100, 100, 1);
    testConsolidateWithLoop("test consolidate 10 buffers each with 100m, 50% used for 10 loop",
        alloc, 10, 1024 * 1024 * 100, 50, 10);
    testConsolidateWithLoop("test consolidate 10 buffers each with 100m, 100% used for 10 loop",
        alloc, 10, 1024 * 1024 * 100, 100, 10);
    testConsolidateWithLoop("test consolidate 10 buffers each with 100m, 50% used for 50 loop",
        alloc, 10, 1024 * 1024 * 100, 50, 50);
    testConsolidateWithLoop("test consolidate 10 buffers each with 100m, 100% used for 50 loop",
        alloc, 10, 1024 * 1024 * 100, 100, 50);
  }

  @Test
  public void testInterception() throws Exception {
    int interceptedReads = 3;
    TransportFrameDecoder decoder = new TransportFrameDecoder();
    TransportFrameDecoder.Interceptor interceptor = spy(new MockInterceptor(interceptedReads));
    ChannelHandlerContext ctx = mockChannelHandlerContext();

    byte[] data = new byte[8];
    ByteBuf len = Unpooled.copyLong(8 + data.length);
    ByteBuf dataBuf = Unpooled.wrappedBuffer(data);

    try {
      decoder.setInterceptor(interceptor);
      for (int i = 0; i < interceptedReads; i++) {
        decoder.channelRead(ctx, dataBuf);
        assertEquals(0, dataBuf.refCnt());
        dataBuf = Unpooled.wrappedBuffer(data);
      }
      decoder.channelRead(ctx, len);
      decoder.channelRead(ctx, dataBuf);
      verify(interceptor, times(interceptedReads)).handle(any(ByteBuf.class));
      verify(ctx).fireChannelRead(any(ByteBuf.class));
      assertEquals(0, len.refCnt());
      assertEquals(0, dataBuf.refCnt());
    } finally {
      release(len);
      release(dataBuf);
    }
  }

  @Test
  public void testRetainedFrames() throws Exception {
    TransportFrameDecoder decoder = new TransportFrameDecoder();

    AtomicInteger count = new AtomicInteger();
    List<ByteBuf> retained = new ArrayList<>();

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.fireChannelRead(any())).thenAnswer(in -> {
      // Retain a few frames but not others.
      ByteBuf buf = (ByteBuf) in.getArguments()[0];
      if (count.incrementAndGet() % 2 == 0) {
        retained.add(buf);
      } else {
        buf.release();
      }
      return null;
    });

    ByteBuf data = createAndFeedFrames(100, decoder, ctx);
    try {
      // Verify all retained buffers are readable.
      for (ByteBuf b : retained) {
        byte[] tmp = new byte[b.readableBytes()];
        b.readBytes(tmp);
        b.release();
      }
      verifyAndCloseDecoder(decoder, ctx, data);
    } finally {
      for (ByteBuf b : retained) {
        release(b);
      }
    }
  }

  @Test
  public void testSplitLengthField() throws Exception {
    byte[] frame = new byte[1024 * (RND.nextInt(31) + 1)];
    ByteBuf buf = Unpooled.buffer(frame.length + 8);
    buf.writeLong(frame.length + 8);
    buf.writeBytes(frame);

    TransportFrameDecoder decoder = new TransportFrameDecoder();
    ChannelHandlerContext ctx = mockChannelHandlerContext();
    try {
      decoder.channelRead(ctx, buf.readSlice(RND.nextInt(7)).retain());
      verify(ctx, never()).fireChannelRead(any(ByteBuf.class));
      decoder.channelRead(ctx, buf);
      verify(ctx).fireChannelRead(any(ByteBuf.class));
      assertEquals(0, buf.refCnt());
    } finally {
      decoder.channelInactive(ctx);
      release(buf);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeFrameSize() throws Exception {
    testInvalidFrame(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyFrame() throws Exception {
    // 8 because frame size includes the frame length.
    testInvalidFrame(8);
  }

  /**
   * Creates a number of randomly sized frames and feed them to the given decoder, verifying
   * that the frames were read.
   */
  private ByteBuf createAndFeedFrames(
      int frameCount,
      TransportFrameDecoder decoder,
      ChannelHandlerContext ctx) throws Exception {
    ByteBuf data = Unpooled.buffer();
    for (int i = 0; i < frameCount; i++) {
      byte[] frame = new byte[1024 * (RND.nextInt(31) + 1)];
      data.writeLong(frame.length + 8);
      data.writeBytes(frame);
    }

    try {
      while (data.isReadable()) {
        int size = RND.nextInt(4 * 1024) + 256;
        decoder.channelRead(ctx, data.readSlice(Math.min(data.readableBytes(), size)).retain());
      }

      verify(ctx, times(frameCount)).fireChannelRead(any(ByteBuf.class));
    } catch (Exception e) {
      release(data);
      throw e;
    }
    return data;
  }

  private void verifyAndCloseDecoder(
      TransportFrameDecoder decoder,
      ChannelHandlerContext ctx,
      ByteBuf data) throws Exception {
    try {
      decoder.channelInactive(ctx);
      assertTrue("There shouldn't be dangling references to the data.", data.release());
    } finally {
      release(data);
    }
  }

  private void testInvalidFrame(long size) throws Exception {
    TransportFrameDecoder decoder = new TransportFrameDecoder();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    ByteBuf frame = Unpooled.copyLong(size);
    try {
      decoder.channelRead(ctx, frame);
    } finally {
      release(frame);
    }
  }

  private ChannelHandlerContext mockChannelHandlerContext() {
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.fireChannelRead(any())).thenAnswer(in -> {
      ByteBuf buf = (ByteBuf) in.getArguments()[0];
      buf.release();
      return null;
    });
    return ctx;
  }

  private void release(ByteBuf buf) {
    if (buf.refCnt() > 0) {
      buf.release(buf.refCnt());
    }
  }

  private static class MockInterceptor implements TransportFrameDecoder.Interceptor {

    private int remainingReads;

    MockInterceptor(int readCount) {
      this.remainingReads = readCount;
    }

    @Override
    public boolean handle(ByteBuf data) throws Exception {
      data.readerIndex(data.readerIndex() + data.readableBytes());
      assertFalse(data.isReadable());
      remainingReads -= 1;
      return remainingReads != 0;
    }

    @Override
    public void exceptionCaught(Throwable cause) throws Exception {

    }

    @Override
    public void channelInactive() throws Exception {

    }

  }

}
