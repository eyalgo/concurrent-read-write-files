package eyalgo.concurrency.files.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

import com.google.gson.Gson;

import eyalgo.concurrency.files.data.User;

public class UsersCreator {
	private final static Gson gson = new Gson();

	public UsersCreator() {
	}

	public static void main(String[] args) throws IOException {
		String path = "src//main//resources//files";

		int numberOfFiles = 200;
		int maxNumberOfUserInFile = 1000;
		for (int i = 0; i < numberOfFiles; i++) {
			Random random = new Random(System.currentTimeMillis());

			String fileOutput = path + File.separator + "users-" + i + ".json";
			Path fileOutputPath = Paths.get(fileOutput);
			BufferedWriter writer = Files.newBufferedWriter(fileOutputPath, Charset.defaultCharset());
			writer.append(gson.toJson(createRandomUsers(random.nextInt(maxNumberOfUserInFile) + 1)));
			writer.flush();

		}
	}

	private static Collection<User> createRandomUsers(int numberOfUsers) {
		Collection<User> users = new ArrayList<>();
		for (int i = 0; i < numberOfUsers; i++) {
			users.add(new User(RandomStringUtils.randomAlphabetic(4), RandomStringUtils.randomAlphabetic(8)));
		}
		return users;
	}

}
