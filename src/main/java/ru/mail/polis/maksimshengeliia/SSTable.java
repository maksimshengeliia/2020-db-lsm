package ru.mail.polis.maksimshengeliia;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class SSTable implements Table {
    private final FileChannel fileChannel;
    private final int rows;
    private final long fileSize;

    /**
     * Constructor of SSTable.
     * @param file main file that represents the database
     * */
    public SSTable(@NotNull final File file) throws IOException {
        this.fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        this.fileSize = fileChannel.size();
        final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(buf, this.fileSize - Integer.BYTES);
        this.rows = buf.rewind().getInt();
    }

    /**
     * Method save data to temporary file before it will be written to main file.
     * @param file temporary file
     * @param cellIterator cell iterator
     * @param rows count of rows
     * */
    public static void serialize(final File file, final Iterator<Cell> cellIterator,
                                 final int rows) throws IOException {
        try (FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW)) {
            final ByteBuffer offsets = ByteBuffer.allocate(rows * Long.BYTES);
            while (cellIterator.hasNext()) {
                offsets.putLong(fc.position());

                final Cell cell = cellIterator.next();
                final ByteBuffer key = cell.getKey();
                final Value value = cell.getValue();

                fc.write(ByteBuffer.allocate(Integer.BYTES).putInt(key.remaining()).rewind());
                fc.write(key.rewind());
                if (value.isRemoved()) {
                    fc.write(ByteBuffer.allocate(Long.BYTES)
                            .putLong(-1 * value.getTimestamp()).rewind());
                } else {
                    fc.write(ByteBuffer.allocate(Long.BYTES)
                            .putLong(value.getTimestamp()).rewind());
                    fc.write(ByteBuffer.allocate(Integer.BYTES)
                            .putInt(value.getData().remaining()).rewind());
                    fc.write(value.getData().rewind());
                }
            }

            fc.write(offsets.rewind());
            fc.write(ByteBuffer.allocate(Integer.BYTES).putInt(rows).rewind());
        }
    }

    private long getOffset(final int row) throws IOException {
        final ByteBuffer offset = ByteBuffer.allocate(Long.BYTES);
        fileChannel.read(offset, this.fileSize - Integer.BYTES - Long.BYTES * (rows - row));
        return offset.rewind().getLong();
    }

    @NotNull
    private ByteBuffer key(final int row) throws IOException {
        assert 0 <= row && row <= rows;
        final long offset = getOffset(row);

        final ByteBuffer keyLengthBuffer = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(keyLengthBuffer, offset);

        final ByteBuffer keyBuffer = ByteBuffer.allocate(keyLengthBuffer.rewind().getInt());
        fileChannel.read(keyBuffer, offset + Integer.BYTES);
        return keyBuffer.rewind();
    }

    @NotNull
    private Value value(final int row) throws IOException {
        assert 0 <= row && row <= rows;
        final long offset = getOffset(row);

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(buffer, offset);
        final int keyLength = buffer.rewind().getInt();

        buffer = ByteBuffer.allocate(Long.BYTES);
        fileChannel.read(buffer, offset + Integer.BYTES + keyLength);
        final long timestamp = buffer.rewind().getLong();

        buffer = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(buffer, offset + Integer.BYTES + keyLength + Long.BYTES);
        final int valueLength = buffer.rewind().getInt();

        buffer = ByteBuffer.allocate(valueLength);
        fileChannel.read(buffer, offset + Integer.BYTES + keyLength + Long.BYTES + Integer.BYTES);

        if (timestamp >= 0) {
            return new Value(timestamp, buffer.rewind());
        } else {
            return new Value(-timestamp, null);
        }
    }

    private int binarySearch(final ByteBuffer from) throws IOException {
        assert rows > 0;

        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = (right + left) / 2;
            final int cmp = from.compareTo(key(mid));
            if (cmp < 0) {
                right = mid - 1;
            } else if (cmp > 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<>() {
            private int next = binarySearch(from);

            @Override
            public boolean hasNext() {
                return next < rows;
            }

            @Override
            public Cell next() {
                try {
                    return new Cell(key(next), value(next++));
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        };
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException("Immutable!");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("Immutable!");
    }

    @Override
    public long sizeInBytes() {
        return fileSize;
    }

    @Override
    public int size() {
        return rows;
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}
