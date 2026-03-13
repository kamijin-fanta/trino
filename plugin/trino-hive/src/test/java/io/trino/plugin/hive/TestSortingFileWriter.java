/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.List;

import static io.trino.plugin.hive.HiveTestUtils.PAGE_SORTER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class TestSortingFileWriter
{
    @Test
    void testTempFileReaderHandlesEofCheckpointAtCommit()
    {
        TrinoFileSystem fileSystem = new MemoryFileSystemFactory().create(ConnectorIdentity.ofUser("test"));
        List<Type> types = ImmutableList.of(VARCHAR);
        CollectingFileWriter outputWriter = new CollectingFileWriter();

        SortingFileWriter sortingFileWriter = new SortingFileWriter(
                fileSystem,
                Location.of("memory:///tmp/sorting-temp"),
                outputWriter,
                DataSize.ofBytes(1),
                4,
                types,
                ImmutableList.of(0),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                PAGE_SORTER,
                new TypeOperators());

        int rowsInLargePage = 70_000;
        sortingFileWriter.appendRows(createPageWithTrailingEmptyAndNullValues(rowsInLargePage));
        sortingFileWriter.appendRows(createPageWithTrailingEmptyAndNullValues(10));

        assertThatCode(() -> {
            try (Closeable ignored = sortingFileWriter.commit()) {
                // no-op
            }
        }).doesNotThrowAnyException();

        assertThat(outputWriter.isCommitted()).isTrue();
        assertThat(outputWriter.getRowCount()).isEqualTo(rowsInLargePage + 10);
    }

    private static Page createPageWithTrailingEmptyAndNullValues(int rows)
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, rows);
        for (int i = 0; i < rows; i++) {
            if (i < Math.min(rows, 50_000)) {
                VARCHAR.writeString(blockBuilder, "value-" + i + "-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
            }
            else if ((i % 2) == 0) {
                VARCHAR.writeString(blockBuilder, "");
            }
            else {
                blockBuilder.appendNull();
            }
        }
        return new Page(blockBuilder.build());
    }

    private static final class CollectingFileWriter
            implements FileWriter
    {
        private long rowCount;
        private boolean committed;

        @Override
        public long getWrittenBytes()
        {
            return rowCount;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void appendRows(Page dataPage)
        {
            rowCount += dataPage.getPositionCount();
        }

        @Override
        public Closeable commit()
        {
            committed = true;
            return () -> {};
        }

        @Override
        public void rollback()
        {
            committed = false;
            rowCount = 0;
        }

        @Override
        public long getValidationCpuNanos()
        {
            return 0;
        }

        public long getRowCount()
        {
            return rowCount;
        }

        public boolean isCommitted()
        {
            return committed;
        }
    }
}
