package com.cgd.gwskeleton;

import com.cgd.gwskeleton.config.GatewayConfigs;
import com.cgd.gwskeleton.publisher.DataPublisherImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

@SpringBootApplication
public class GwSkeletonApplication implements CommandLineRunner {

	@Autowired
	DataPublisherImpl publisher ;

	@Autowired
    private GatewayConfigs gatewayConfigs;



	public static void main(String[] args) {

		SpringApplication.run(GwSkeletonApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		//runProducer();
		//PrepareFile();

		Thread.sleep(100000);
	}

	void runProducer() {

		System.out.println("Running the producer.......................");

		for (int index = 0; index < gatewayConfigs.MESSAGE_COUNT; index++) {

			publisher.sendMessage("This is record " + index);



		}

	}

	void PrepareFile(){
		long startSn = 1001;

		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(
					new FileWriter("ExternalMessage.txt", true)  //Set true for append mode
			);

			for (long i = startSn ; i < startSn+1000 ; i++) {
				String textToAppend = String.format("%06d-This is the message", i);
				System.out.println(textToAppend);

				writer.write(textToAppend);
                writer.newLine();   //Add new line
			}
			writer.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
