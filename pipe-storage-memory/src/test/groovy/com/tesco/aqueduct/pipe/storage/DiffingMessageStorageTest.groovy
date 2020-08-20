package com.tesco.aqueduct.pipe.storage

import com.tesco.aqueduct.pipe.api.DistributedStorage
import com.tesco.aqueduct.pipe.codec.CodecType
import com.tesco.aqueduct.pipe.repository.DiffingMessageRepository

class DiffingMessageStorageTest extends StorageTckSpec {
    DistributedStorage storage = new RepositoryBasedInMemoryStorage(
        1000,
        new DiffingMessageRepository(true, CodecType.BROTLI)
    )
}
