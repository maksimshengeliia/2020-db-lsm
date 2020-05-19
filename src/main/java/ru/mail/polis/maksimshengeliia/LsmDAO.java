package ru.mail.polis.maksimshengeliia;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Stream;

public class LsmDAO implements DAO {
    private static final Logger log = LoggerFactory.getLogger(LsmDAO.class);

    private static String SUFFIX = ".dat";
    private static String TEMP = ".tmp";

    @NotNull
    private final File storage;
    private final long flushThreshold;

    // Data
    private Table memTable;
    private NavigableMap<Integer, Table> ssTables;

    // State
    private int generation;

    /**
     * DAO Implementation.
     * @param storage database directory
     * @param flushThreshold threshold of data to be writen on disk
     * */
    public LsmDAO(@NotNull final File storage, final long flushThreshold) throws IOException {
        assert flushThreshold > 0L;
        this.storage = storage;
        this.flushThreshold = flushThreshold;
        this.memTable = new MemTable();
        this.ssTables = new TreeMap<>();
        try (Stream<Path> files = Files.list(storage.toPath())) {
            files.filter(path -> path.toString().endsWith(SUFFIX)).forEach(f -> {
                try {
                    final String name = f.getFileName().toString();
                    final int gen = Integer.parseInt(name.substring(0, name.indexOf(SUFFIX)));
                    generation = Math.max(generation, gen);
                    ssTables.put(gen, new SSTable(f.toFile()));
                } catch (IOException e) {
                    log.info("IOException in 'new SSTable'");
                } catch (NumberFormatException e) {
                    log.info("Incorrect file name");
                }
            });
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iterations = new ArrayList<>(ssTables.size() + 1);
        iterations.add(memTable.iterator(from));
        for (final Table t : ssTables.descendingMap().values()) {
            iterations.add(t.iterator(from));
        }
        final Iterator<Cell> merged = Iterators.mergeSorted(iterations, Cell.COMPARATOR);
        final Iterator<Cell> fresh = Iters.collapseEquals(merged, Cell::getKey);
        final Iterator<Cell> alive = Iterators.filter(fresh, e -> !e.getValue().isRemoved());
        return Iterators.transform(alive, e -> Record.of(e.getKey(), e.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    private void flush() throws IOException {
        // Dump memTable
        final File tempFile = new File(storage, generation + TEMP);
        SSTable.serialize(tempFile, memTable.iterator(ByteBuffer.allocate(0)), memTable.size());
        final File destFile = new File(storage, generation + SUFFIX);
        Files.move(tempFile.toPath(), destFile.toPath(), StandardCopyOption.ATOMIC_MOVE);

        // Switch
        memTable = new MemTable();
        ssTables.put(generation, new SSTable(destFile));
        generation++;
    }

    @Override
    public void close() throws IOException {
        if (memTable.size() > 0) {
            flush();
        }

        for (final Table t : ssTables.values()) {
            t.close();
        }
    }

    @Override
    public void compact() throws IOException {
        final File tempFile = new File(storage, generation + TEMP);
        SSTable.serialize(tempFile, memTable.iterator(ByteBuffer.allocate(0)), memTable.size());
        for (int i = 0; i < generation; i++) {
            Files.delete(new File(storage, i + SUFFIX).toPath());
        }
        generation = 0;
        final File file = new File(storage, generation + SUFFIX);
        Files.move(tempFile.toPath(), file.toPath(), StandardCopyOption.ATOMIC_MOVE);
        ssTables = new TreeMap<>();
        ssTables.put(generation++, new SSTable(file));
        memTable = new MemTable();
    }
}
