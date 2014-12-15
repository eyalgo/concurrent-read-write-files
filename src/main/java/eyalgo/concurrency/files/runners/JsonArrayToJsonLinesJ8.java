package eyalgo.concurrency.files.runners;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import com.google.gson.Gson;

import eyalgo.concurrency.files.data.User;

public class JsonArrayToJsonLinesJ8 {
	private final static Path inputFilesDirectory = Paths.get("src\\main\\resources\\files");
	private final static Path outputDirectory = Paths.get("src\\main\\resources\\files\\output");
	private final static Gson gson = new Gson();

	private final BlockingQueue<EntitiesData> entitiesQueue = new LinkedBlockingQueue<>();

	private AtomicBoolean stillWorking = new AtomicBoolean(true);
	private Semaphore semaphore = new Semaphore(0);
	int numberOfFiles = 0;

	private JsonArrayToJsonLinesJ8() {
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		new JsonArrayToJsonLinesJ8().process();
	}

	private void process() throws IOException, InterruptedException {
		deleteFilesInOutputDir();
		final ExecutorService executorService = createExecutorService();
		DirectoryStream<Path> directoryStream = Files.newDirectoryStream(inputFilesDirectory, "*.json");

		for (int i = 0; i < 2; i++) {
			new Thread(new JsonElementsFileWriter(stillWorking, semaphore, entitiesQueue)).start();
		}

		Consumer<? super Path> action = (path) -> {
			numberOfFiles++;
			executorService.submit(() -> {
				Path fileName = path.getFileName();
				try {
					BufferedReader br = Files.newBufferedReader(path);
					User[] entities = gson.fromJson(br, User[].class);
					System.out.println("Done reading " + fileName);
					entitiesQueue.put(new EntitiesData(fileName.toString(), entities));
				} catch (IOException | InterruptedException e) {
					throw new RuntimeException(path.toString(), e);
				}
			});
		};

		StreamSupport.stream(directoryStream.spliterator(), true).parallel().forEach(action);

		// directoryStream.forEach(path -> {
		// numberOfFiles++;
		// executorService.submit(() -> {
		// Path fileName = path.getFileName();
		// try {
		// BufferedReader br = Files.newBufferedReader(path);
		// User[] entities = gson.fromJson(br, User[].class);
		// System.out.println("---> " + fileName);
		// entitiesQueue.put(new EntitiesData(fileName.toString(), entities));
		// } catch (IOException | InterruptedException e) {
		// throw new RuntimeException(path.toString(), e);
		// }
		// });
		// });

		semaphore.acquire(numberOfFiles);
		stillWorking.set(false);
		shutDownExecutor(executorService);
	}

	private void deleteFilesInOutputDir() throws IOException {
		Files.walkFileTree(outputDirectory, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}
		});
	}

	private ExecutorService createExecutorService() {
		return Executors.newFixedThreadPool(2);
	}

	private void shutDownExecutor(final ExecutorService executorService) {
		executorService.shutdown();
		try {
			if (!executorService.awaitTermination(120, TimeUnit.SECONDS)) {
				executorService.shutdownNow();
			}

			if (!executorService.awaitTermination(120, TimeUnit.SECONDS)) {
			}
		} catch (InterruptedException ex) {
			executorService.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	private static final class JsonElementsFileWriter implements Runnable {
		private final BlockingQueue<EntitiesData> entitiesQueue;
		private final AtomicBoolean stillWorking;
		private final Semaphore semaphore;

		private JsonElementsFileWriter(AtomicBoolean stillWorking, Semaphore semaphore,
				BlockingQueue<EntitiesData> entitiesQueue) {
			this.stillWorking = stillWorking;
			this.semaphore = semaphore;
			this.entitiesQueue = entitiesQueue;
		}

		@Override
		public void run() {
			while (stillWorking.get()) {
				try {
					EntitiesData data = entitiesQueue.poll(100, TimeUnit.MILLISECONDS);
					if (data != null) {
						try {
							String fileOutput = outputDirectory.toString() + File.separator + data.fileName;
							Path fileOutputPath = Paths.get(fileOutput);
							BufferedWriter writer = Files.newBufferedWriter(fileOutputPath, Charset.defaultCharset());
							writeLines(data, writer);
							writer.flush();
							System.out.println("Done writing " + data.fileName);
						} catch (IOException e) {
							throw new RuntimeException(data.fileName, e);
						} finally {
							semaphore.release();
						}
					}
				} catch (InterruptedException e1) {
				}
			}
		}

		private void writeLines(EntitiesData data, BufferedWriter writer) {
			Arrays.asList(data.entities).parallelStream().forEach(user -> {
				try {
					writer.append(gson.toJson(user));
					writer.newLine();
				} catch (Exception e) {
					throw new RuntimeException(data.fileName, e);
				}
			});
		}
	}

	private static final class EntitiesData {
		private final String fileName;
		private final User[] entities;

		private EntitiesData(String fileName, User[] entities) {
			this.fileName = fileName;
			this.entities = entities;
		}
	}
}
