package com.mycompany.agent;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The {@code getFileInfo} skill: stats an array of paths on this host and reports, per path,
 * whether it exists plus its name, type, timestamps, size and permissions — in one reply, so a
 * caller can check a whole set of files (a drop folder's expected outputs, a deployment's jars)
 * and analyze them side by side.
 *
 * <p>Each path is answered on its own: one unreadable or missing path becomes that entry's
 * {@code exists:false} / {@code error}, never a failure of the whole call. Symlinks are reported
 * as links (not followed) so a dangling link still shows up, with its target alongside.
 */
public class FileInfoCollector {

    /** Guard against a caller pasting a whole filesystem listing into one request. */
    private static final int MAX_PATHS = 5_000;

    private static final boolean POSIX = java.nio.file.FileSystems.getDefault()
            .supportedFileAttributeViews().contains("posix");

    public void execute(List<String> paths, Consumer<Map<String, Object>> onDone) {
        Thread.ofVirtual().name("file-info").start(() -> onDone.accept(collect(paths)));
    }

    public Map<String, Object> collect(List<String> paths) {
        long start = System.currentTimeMillis();
        List<String> requested = paths != null ? paths : List.of();
        Map<String, Object> result = new LinkedHashMap<>();
        if (requested.size() > MAX_PATHS) {
            result.put("error", "Too many paths: " + requested.size() + " (max " + MAX_PATHS + ")");
            result.put("durationMs", System.currentTimeMillis() - start);
            return result;
        }

        List<Map<String, Object>> files = new ArrayList<>(requested.size());
        for (String raw : requested) files.add(stat(raw));

        result.put("files", files);
        result.put("count", files.size());
        result.put("durationMs", System.currentTimeMillis() - start);
        return result;
    }

    private Map<String, Object> stat(String raw) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("path", raw);
        if (raw == null || raw.isBlank()) {
            info.put("exists", false);
            info.put("error", "empty path");
            return info;
        }

        Path path;
        try {
            path = Paths.get(expandHome(raw.trim()));
        } catch (Exception e) {
            info.put("exists", false);
            info.put("error", "invalid path: " + e.getMessage());
            return info;
        }

        try {
            info.put("absolutePath", path.toAbsolutePath().normalize().toString());
            Path fileName = path.getFileName();
            String name = fileName != null ? fileName.toString() : path.toString();
            info.put("name", name);

            if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
                info.put("exists", false);
                return info;
            }
            info.put("exists", true);

            BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            info.put("type", typeOf(attrs));
            if (attrs.isSymbolicLink()) {
                info.put("linkTarget", Files.readSymbolicLink(path).toString());
                info.put("linkTargetExists", Files.exists(path));
            }
            info.put("extension", attrs.isRegularFile() ? extensionOf(name) : "");
            info.put("size", attrs.size());
            info.put("created", iso(attrs.creationTime()));
            info.put("modified", iso(attrs.lastModifiedTime()));
            info.put("accessed", iso(attrs.lastAccessTime()));
            info.put("ageSeconds", (System.currentTimeMillis() - attrs.lastModifiedTime().toMillis()) / 1000);

            if (attrs.isRegularFile()) {
                try { info.put("contentType", Files.probeContentType(path)); } catch (Exception ignored) {}
            }

            info.put("readable", Files.isReadable(path));
            info.put("writable", Files.isWritable(path));
            info.put("executable", Files.isExecutable(path));
            info.put("hidden", isHidden(path));
            addPermissions(path, info);
        } catch (Exception e) {
            info.putIfAbsent("exists", Files.exists(path, LinkOption.NOFOLLOW_LINKS));
            info.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        return info;
    }

    /**
     * POSIX hosts get the familiar {@code rwxr-x---} string plus owner/group and octal mode; on
     * Windows the closest honest equivalent is the owner and the DOS attribute flags.
     */
    private void addPermissions(Path path, Map<String, Object> info) {
        try {
            if (POSIX) {
                PosixFileAttributes posix = Files.readAttributes(path, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                String perms = PosixFilePermissions.toString(posix.permissions());
                info.put("permissions", perms);
                info.put("mode", toOctal(perms));
                info.put("owner", posix.owner().getName());
                info.put("group", posix.group().getName());
            } else {
                FileOwnerAttributeView owner = Files.getFileAttributeView(path, FileOwnerAttributeView.class, LinkOption.NOFOLLOW_LINKS);
                if (owner != null) info.put("owner", owner.getOwner().getName());
                DosFileAttributes dos = Files.readAttributes(path, DosFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                StringBuilder flags = new StringBuilder();
                if (dos.isReadOnly()) flags.append('R');
                if (dos.isHidden())   flags.append('H');
                if (dos.isSystem())   flags.append('S');
                if (dos.isArchive())  flags.append('A');
                info.put("permissions", (Files.isReadable(path) ? "r" : "-")
                        + (Files.isWritable(path) ? "w" : "-")
                        + (Files.isExecutable(path) ? "x" : "-"));
                info.put("attributes", flags.toString());
            }
        } catch (Exception e) {
            info.put("permissionsError", e.getMessage());
        }
    }

    private static String typeOf(BasicFileAttributes attrs) {
        if (attrs.isSymbolicLink()) return "symlink";
        if (attrs.isDirectory())    return "directory";
        if (attrs.isRegularFile())  return "file";
        return "other";
    }

    private static String extensionOf(String name) {
        int dot = name.lastIndexOf('.');
        return dot > 0 && dot < name.length() - 1 ? name.substring(dot + 1).toLowerCase() : "";
    }

    private static boolean isHidden(Path path) {
        try { return Files.isHidden(path); } catch (Exception e) { return false; }
    }

    private static String toOctal(String perms) {
        int mode = 0;
        for (int i = 0; i < 9; i++) if (perms.charAt(i) != '-') mode |= 1 << (8 - i);
        return String.format("%03o", mode);
    }

    private static String iso(FileTime time) {
        return time != null ? time.toInstant().toString() : null;
    }

    private static String expandHome(String raw) {
        if (raw.equals("~") || raw.startsWith("~/") || raw.startsWith("~\\")) {
            return System.getProperty("user.home") + raw.substring(1);
        }
        return raw;
    }
}
