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
package io.trino.orc.stream;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcDecompressor;
import io.trino.orc.checkpoint.ByteArrayStreamCheckpoint;
import io.trino.orc.metadata.OrcColumnId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcDecompressor.createOrcDecompressor;
import static io.trino.orc.checkpoint.InputStreamCheckpoint.decodeCompressedBlockOffset;
import static io.trino.orc.checkpoint.InputStreamCheckpoint.decodeDecompressedOffset;
import static io.trino.orc.metadata.CompressionKind.SNAPPY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class TestByteArrayStream
        extends AbstractTestValueStream<Slice, ByteArrayStreamCheckpoint, ByteArrayOutputStream, ByteArrayInputStream>
{
    @Test
    public void test()
            throws IOException
    {
        List<List<Slice>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Slice> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                Slice value = Slices.allocate(8);
                SliceOutput output = value.getOutput();
                output.writeInt(groupIndex);
                output.writeInt(i);
                group.add(value);
            }
            groups.add(group);
        }
        testWriteValue(groups);
    }

    @Test
    public void testSeekToCheckpointAtCompressedStreamEnd()
            throws IOException
    {
        ByteArrayOutputStream outputStream = createValueOutputStream();

        outputStream.recordCheckpoint();
        Slice largeValue = Slices.allocate(40_000);
        outputStream.writeSlice(largeValue);
        outputStream.writeSlice(largeValue);

        // Record an empty trailing group, so its checkpoint points at EOF.
        outputStream.recordCheckpoint();
        outputStream.close();

        List<ByteArrayStreamCheckpoint> checkpoints = outputStream.getCheckpoints();
        assertThat(checkpoints).hasSize(2);

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1000);
        StreamDataOutput streamDataOutput = outputStream.getStreamDataOutput(new OrcColumnId(33));
        streamDataOutput.writeData(sliceOutput);

        long emptyGroupCheckpoint = checkpoints.get(1).getInputStreamCheckpoint();
        assertThat(decodeCompressedBlockOffset(emptyGroupCheckpoint)).isEqualTo(streamDataOutput.getStream().getLength());
        assertThat(decodeDecompressedOffset(emptyGroupCheckpoint)).isZero();

        ByteArrayInputStream valueStream = createValueStream(sliceOutput.slice());
        assertThatCode(() -> valueStream.seekToCheckpoint(checkpoints.get(1))).doesNotThrowAnyException();
    }

    @Override
    protected ByteArrayOutputStream createValueOutputStream()
    {
        return new ByteArrayOutputStream(SNAPPY, COMPRESSION_BLOCK_SIZE);
    }

    @Override
    protected void writeValue(ByteArrayOutputStream outputStream, Slice value)
    {
        outputStream.writeSlice(value);
    }

    @Override
    protected ByteArrayInputStream createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, COMPRESSION_BLOCK_SIZE);
        return new ByteArrayInputStream(new OrcInputStream(OrcChunkLoader.create(ORC_DATA_SOURCE_ID, slice, orcDecompressor, newSimpleAggregatedMemoryContext())));
    }

    @Override
    protected Slice readValue(ByteArrayInputStream valueStream)
            throws IOException
    {
        return Slices.wrappedBuffer(valueStream.next(8));
    }
}
