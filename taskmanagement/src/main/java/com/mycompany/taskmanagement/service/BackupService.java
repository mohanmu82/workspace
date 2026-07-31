package com.mycompany.taskmanagement.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Backs up / restores everything under app.data.base-directory as a single zip so the
 * JSON-file data tree (tasks, projects, programmes, events, templates, field configs, ...)
 * can be recreated if the filesystem it lives on is wiped out.
 */
@Service
public class BackupService {

    private final Path baseDirectory;

    public BackupService(@Value("${app.data.base-directory:./data}") String baseDirectory) {
        this.baseDirectory = Paths.get(baseDirectory).toAbsolutePath().normalize();
    }

    public String backupFileName() {
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        return "taskmanagement-backup-" + ts + ".zip";
    }

    public void exportTo(OutputStream out) throws IOException {
        Files.createDirectories(baseDirectory);
        try (ZipOutputStream zos = new ZipOutputStream(out);
             Stream<Path> walk = Files.walk(baseDirectory)) {
            Stream<Path> regularFiles = walk.filter(Files::isRegularFile);
            for (Path path : (Iterable<Path>) regularFiles::iterator) {
                String entryName = baseDirectory.relativize(path).toString().replace('\\', '/');
                zos.putNextEntry(new ZipEntry(entryName));
                Files.copy(path, zos);
                zos.closeEntry();
            }
        }
    }

    public void importFrom(MultipartFile file) throws IOException {
        Files.createDirectories(baseDirectory);
        try (InputStream in = file.getInputStream();
             ZipInputStream zis = new ZipInputStream(in)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    zis.closeEntry();
                    continue;
                }
                Path target = resolveSafely(entry.getName());
                if (target.getParent() != null) {
                    Files.createDirectories(target.getParent());
                }
                Files.copy(zis, target, StandardCopyOption.REPLACE_EXISTING);
                zis.closeEntry();
            }
        }
    }

    private Path resolveSafely(String entryName) {
        Path target = baseDirectory.resolve(entryName).normalize();
        if (!target.startsWith(baseDirectory)) {
            throw new IllegalArgumentException("Zip entry escapes data directory: " + entryName);
        }
        return target;
    }
}
