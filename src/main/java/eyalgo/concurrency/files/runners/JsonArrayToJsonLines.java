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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.google.gson.Gson;

import eyalgo.concurrency.files.data.User;

public class JsonArrayToJsonLines {
	private final static Path inputFilesDirectory = Paths.get("src\\main\\resources\\files");
	private final static Path outputDirectory = Paths
			.get("src\\main\\resources\\files\\output");
	private final static Gson gson = new Gson();
	
	private final BlockingQueue<EntitiesData> entitiesQueue = new LinkedBlockingQueue<>();
	
	private AtomicBoolean stillWorking = new AtomicBoolean(true);
	private Semaphore semaphore = new Semaphore(0);
	int numberOfFiles = 0;

	private JsonArrayToJsonLines() {
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		new JsonArrayToJsonLines().process();
	}

	private void process() throws IOException, InterruptedException {
		deleteFilesInOutputDir();
		final ExecutorService executorService = createExecutorService();
		DirectoryStream<Path> directoryStream = Files.newDirectoryStream(inputFilesDirectory, "*.json");
		
		for (int i = 0; i < 2; i++) {
			new Thread(new JsonElementsFileWriter(stillWorking, semaphore, entitiesQueue)).start();
		}

		directoryStream.forEach(new Consumer<Path>() {
			@Override
			public void accept(Path filePath) {
				numberOfFiles++;
				executorService.submit(new OriginalFileReader(filePath, entitiesQueue));
			}
		});
		
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
		int numberOfCpus = Runtime.getRuntime().availableProcessors();
		return Executors.newFixedThreadPool(numberOfCpus);
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


	private static final class OriginalFileReader implements Runnable {
		private final Path filePath;
		private final BlockingQueue<EntitiesData> entitiesQueue;

		private OriginalFileReader(Path filePath, BlockingQueue<EntitiesData> entitiesQueue) {
			this.filePath = filePath;
			this.entitiesQueue = entitiesQueue;
		}

		@Override
		public void run() {
			Path fileName = filePath.getFileName();
			try {
				BufferedReader br = Files.newBufferedReader(filePath);
				User[] entities = gson.fromJson(br, User[].class);
				System.out.println("---> " + fileName);
				entitiesQueue.put(new EntitiesData(fileName.toString(), entities));
			} catch (IOException | InterruptedException e) {
				throw new RuntimeException(filePath.toString(), e);
			}
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
							for (User user : data.entities) {
								writer.append(gson.toJson(user));
								writer.newLine();
							}
							writer.flush();
							System.out.println("=======================================>>>>> " + data.fileName);
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
