package com.example.imageanalyzer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

@SpringBootApplication
public class ImageAnalyzerApp implements CommandLineRunner {

    private static final Set<String> IMAGE_EXTENSIONS = Set.of(
            "jpg", "jpeg", "png", "gif", "bmp", "tiff", "tif", "webp"
    );
    private static final Set<String> THUMBNAIL_KEYWORDS = Set.of(
            "thumb", "thumbnail", "small", "tiny", "icon"
    );
    private static final int THUMBNAIL_SIZE_THRESHOLD = 128;
    private static final int MAX_CONCURRENT_IMAGES = 16; // Based on available memory
    private static final int HASH_THUMBNAIL_SIZE = 32; // Size for downscaled hash image

    public static void main(String[] args) {
        SpringApplication.run(ImageAnalyzerApp.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java -jar image-analyzer.jar <source-directory> [<target-base-directory>]");
            System.exit(1);
        }

        Path sourceDir = Paths.get(args[0]);
        Path targetBaseDir = args.length > 1 ? Paths.get(args[1]) : sourceDir;

        if (!Files.isDirectory(sourceDir)) {
            System.err.println("Source directory doesn't exist: " + sourceDir);
            System.exit(1);
        }

        Path validDir = targetBaseDir.resolve("valid_images");
        Path invalidDir = targetBaseDir.resolve("invalid_images");
        Path duplicatesDir = targetBaseDir.resolve("duplicate_images");

        createDirectories(validDir, invalidDir, duplicatesDir);

        ConcurrentHashMap<String, String> imageHashes = new ConcurrentHashMap<>();
        processImagesConcurrently(sourceDir, validDir, invalidDir, duplicatesDir, imageHashes);
    }

    private void createDirectories(Path... directories) throws IOException {
        for (Path dir : directories) {
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
        }
    }

    private void processImagesConcurrently(Path sourceDir, Path validDir, Path invalidDir,
                                           Path duplicatesDir, ConcurrentMap<String, String> imageHashes) {
        Semaphore processingSemaphore = new Semaphore(MAX_CONCURRENT_IMAGES);
        ExecutorService executor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("image-processor-", 0).factory()
        );

        try (Stream<Path> pathStream = Files.walk(sourceDir)) {
            List<CompletableFuture<Void>> futures = pathStream
                    .filter(Files::isRegularFile)
                    .filter(this::isImageFile)
                    .map(file -> processFileAsync(file, sourceDir, validDir, invalidDir, duplicatesDir, imageHashes, processingSemaphore, executor))
                    .toList();

            // Wait for all tasks to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (IOException e) {
            System.err.println("Error walking directory: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    private CompletableFuture<Void> processFileAsync(Path file, Path sourceDir, Path validDir,
                                                     Path invalidDir, Path duplicatesDir,
                                                     ConcurrentMap<String, String> imageHashes,
                                                     Semaphore semaphore, ExecutorService executor) {
        return CompletableFuture.runAsync(() -> {
            try {
                semaphore.acquire();
                analyzeImage(file, sourceDir, validDir, invalidDir, duplicatesDir, imageHashes);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("Error processing: " + file + " - " + e.getMessage());
            } finally {
                semaphore.release();
            }
        }, executor);
    }

    private boolean isImageFile(Path file) {
        String fileName = file.getFileName().toString().toLowerCase();
        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex == -1) return false;
        String extension = fileName.substring(dotIndex + 1);
        return IMAGE_EXTENSIONS.contains(extension);
    }

    private void analyzeImage(Path imagePath, Path sourceDir, Path validDir,
                              Path invalidDir, Path duplicatesDir,
                              ConcurrentMap<String, String> imageHashes) throws IOException {
        // Check for thumbnail by filename
        String fileName = imagePath.getFileName().toString().toLowerCase();
        if (isThumbnailByName(fileName)) {
            moveFile(imagePath, sourceDir, invalidDir, "thumbnail");
            return;
        }

        // Check for ICO files
        if (fileName.endsWith(".ico")) {
            moveFile(imagePath, sourceDir, invalidDir, "ico");
            return;
        }

        // Try to read image
        BufferedImage img = null;
        try {
            img = ImageIO.read(imagePath.toFile());
        } catch (OutOfMemoryError e) {
            System.err.println("Memory error processing: " + imagePath + " - " + e.getMessage());
            moveFile(imagePath, sourceDir, invalidDir, "memory_error");
            return;
        } catch (Exception e) {
            // Fall through to null check
        }

        if (img == null) {
            moveFile(imagePath, sourceDir, invalidDir, "corrupt");
            return;
        }

        // Check for thumbnail by size
        if (isThumbnailBySize(img)) {
            moveFile(imagePath, sourceDir, invalidDir, "thumbnail");
            return;
        }

        // Handle valid images with optimized hashing
        String hash = calculateCompactImageHash(img);
        Path relativePath = sourceDir.relativize(imagePath);
        String relativePathStr = relativePath.toString();

        // Thread-safe duplicate detection
        String existingPath = imageHashes.putIfAbsent(hash, relativePathStr);
        if (existingPath != null) {
            moveFile(imagePath, sourceDir, duplicatesDir, "duplicate");
        } else {
            moveFile(imagePath, sourceDir, validDir, "valid");
        }
    }

    private boolean isThumbnailByName(String fileName) {
        return THUMBNAIL_KEYWORDS.stream().anyMatch(fileName::contains);
    }

    private boolean isThumbnailBySize(BufferedImage img) {
        return img.getWidth() <= THUMBNAIL_SIZE_THRESHOLD &&
                img.getHeight() <= THUMBNAIL_SIZE_THRESHOLD;
    }

    private String calculateCompactImageHash(BufferedImage original) {
        // Create downscaled version for efficient hashing
        BufferedImage thumbnail = new BufferedImage(
                HASH_THUMBNAIL_SIZE, HASH_THUMBNAIL_SIZE, BufferedImage.TYPE_INT_RGB
        );

        Graphics2D g = thumbnail.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
                RenderingHints.VALUE_INTERPOLATION_BILINEAR);
        g.drawImage(original, 0, 0, HASH_THUMBNAIL_SIZE, HASH_THUMBNAIL_SIZE, null);
        g.dispose();

        // Compute hash from downscaled image
        int[] pixels = thumbnail.getRGB(0, 0,
                HASH_THUMBNAIL_SIZE, HASH_THUMBNAIL_SIZE, null, 0, HASH_THUMBNAIL_SIZE);

        return Arrays.hashCode(pixels) + "_" +
                original.getWidth() + "x" + original.getHeight();
    }

    private void moveFile(Path source, Path sourceBase, Path targetDir, String category) throws IOException {
        Path relativePath = sourceBase.relativize(source);
        Path target = targetDir.resolve(relativePath);

        Files.createDirectories(target.getParent());

        try {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
            System.out.println("[" + Thread.currentThread().getName() + "] Moved [" + category + "]: " + source + " -> " + target);
        } catch (AccessDeniedException e) {
            System.err.println("Access denied moving file: " + source + " - " + e.getMessage());
        } catch (FileAlreadyExistsException e) {
            System.err.println("File already exists: " + target + " - " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error moving file: " + source + " - " + e.getMessage());
        }
    }
}