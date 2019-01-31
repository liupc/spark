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

package org.apache.spark.storage

import java.io.{File, FileWriter}
import java.util.UUID

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.spark.{SparkConf, SparkFunSuite}

import org.apache.spark.internal.config
import org.apache.spark.util.{ManualClock, Utils}

class DiskBlockManagerSuite extends SparkFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  private val testConf = new SparkConf(false)
  private var rootDir0: File = _
  private var rootDir1: File = _
  private var rootDirs: String = _

  var diskBlockManager: DiskBlockManager = _

  override def beforeAll() {
    super.beforeAll()
    rootDir0 = Utils.createTempDir()
    rootDir1 = Utils.createTempDir()
    rootDirs = rootDir0.getAbsolutePath + "," + rootDir1.getAbsolutePath
  }

  override def afterAll() {
    try {
      Utils.deleteRecursively(rootDir0)
      Utils.deleteRecursively(rootDir1)
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach() {
    super.beforeEach()
    val conf = testConf.clone
    conf.set("spark.local.dir", rootDirs)
    diskBlockManager = new DiskBlockManager(conf, deleteFilesOnStop = true)
  }

  override def afterEach() {
    try {
      diskBlockManager.stop()
    } finally {
      super.afterEach()
    }
  }

  test("basic block creation") {
    val blockId = new TestBlockId("test")
    val newFile = diskBlockManager.getFile(blockId)
    writeToFile(newFile, 10)
    assert(diskBlockManager.containsBlock(blockId))
    newFile.delete()
    assert(!diskBlockManager.containsBlock(blockId))
  }

  test("enumerating blocks") {
    val ids = (1 to 100).map(i => TestBlockId("test_" + i))
    val files = ids.map(id => diskBlockManager.getFile(id))
    files.foreach(file => writeToFile(file, 10))
    assert(diskBlockManager.getAllBlocks.toSet === ids.toSet)
  }

  test("SPARK-22227: non-block files are skipped") {
    val file = diskBlockManager.getFile("unmanaged_file")
    writeToFile(file, 10)
    assert(diskBlockManager.getAllBlocks().isEmpty)
  }

  def writeToFile(file: File, numBytes: Int) {
    val writer = new FileWriter(file, true)
    for (i <- 0 until numBytes) writer.write(i)
    writer.close()
  }

  test(s"test blacklisting bad disk directory") {
    for ((badDiskDir, goodDiskDir) <- Seq((rootDir0, rootDir1), (rootDir1, rootDir0))) {
      val blockId1 = TestBlockId("1")
      val blockId2 = TestBlockId("2")
      val blockId3 = TestBlockId("3")

      val conf = testConf.clone
      conf.set("spark.local.dir", rootDirs)
      conf.set(config.DISK_STORE_BLACKLIST_TIMEOUT.key, "10000")
      val manualClock = new ManualClock(10000L)
      val diskBlockManager = new DiskBlockManager(conf, true, manualClock)

      // Get file succeed when no disk turns bad
      val file1 = diskBlockManager.getFile(blockId1)
      assert(file1 != null)

      // Delete badDiskDir to simulate disk broken
      Utils.deleteRecursively(badDiskDir)

      // Get new file succeed when single disk is broken
      try {
        val file2 = diskBlockManager.getFile(blockId2)
        val rootDirOfFile2 = file2.getParentFile.getParentFile.getParentFile
        assert(file2 != null && file2.getParentFile.exists() && rootDirOfFile2 === goodDiskDir)
        if (diskBlockManager.badDirs.nonEmpty) {
          assert(diskBlockManager.badDirs.size === 1)
          assert(diskBlockManager.badDirs.exists(_.getParentFile === badDiskDir))
          assert(diskBlockManager.dirToBlacklistExpiryTime.size === 1)
          assert(diskBlockManager.dirToBlacklistExpiryTime.exists { case (f, expireTime) =>
            f.getParentFile === badDiskDir && expireTime === 20000
          })
        }

        // Get file succeed after bad disk blacklisted
        val file3 = diskBlockManager.getFile(blockId1)
        assert(file1 === file3)

        val file4 = diskBlockManager.getFile(blockId2)
        val rootDirOfFile4 = file4.getParentFile.getParentFile.getParentFile
        assert(file4 != null && file4.getParentFile.exists() && rootDirOfFile4 === goodDiskDir)
      } finally {
        diskBlockManager.localDirs.foreach(_.mkdirs())
      }

      manualClock.advance(10000)

      // Update blacklist when getting file for new block
      // Bad disk directory is fixed here, so blacklist should be empty
      assert(diskBlockManager.getFile(blockId3) != null)
      assert(diskBlockManager.badDirs.isEmpty)
      assert(diskBlockManager.dirToBlacklistExpiryTime.isEmpty)
      diskBlockManager.stop()
    }
  }
}
