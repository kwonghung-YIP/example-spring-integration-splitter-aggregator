package org.hung;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.Aggregator;
import org.springframework.integration.annotation.Splitter;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.dsl.Files;
import org.springframework.messaging.Message;

@SpringBootApplication
@EnableIntegration
public class MyExampleApp {

	public static void main(String[] args) {
		SpringApplication.run(MyExampleApp.class, args);
	}

	class SplitterPojo {
		@Splitter
		int[] split(Message msg) {
			return IntStream.range(1, 10).toArray();
		}
	}
	
	class AggregatorPojo {
		@Aggregator
		String aggregate(Collection<Message<File>> messages) {
			return "Done";
		}
	}
	
	@Bean
	public AggregatorPojo aggregator() {
		return new AggregatorPojo();
	}
		
	@Bean
	public IntegrationFlow sampleFlow() {
		return IntegrationFlows
				.from(
					Files
						.inboundAdapter(new File("src/main/resources"))
						.patternFilter("*.zip"),
					e -> e.poller(Pollers.fixedDelay(2000))
						
				)
				.<File>split(zipFile -> {
					List<File> eachFiles = new ArrayList<File>();
					try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFile))) {
						File tmpFolder = FileUtils.getTempDirectory();
						ZipEntry entry = zipIn.getNextEntry();
						while (entry!=null) {
							int len;
							byte buffer[] = new byte[8192];
							File tmpFile = new File(tmpFolder,entry.getName());
							eachFiles.add(tmpFile);
							try (FileOutputStream fout = new FileOutputStream(tmpFile, false)) {
								do {
									len = zipIn.read(buffer);
									if (len>0) {
										fout.write(buffer, 0, len);
									}
								} while (len>0);
							}
							entry = zipIn.getNextEntry();
						}
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					return eachFiles;
				}, e -> { })
				//.<File>log(msg -> msg.getPayload().getName())
				.route(
					File.class,f -> f.getName(),
					r -> r
					.prefix("channel-")
						.channelMapping("1.txt","A")
						.channelMapping("2.txt","B")
						.channelMapping("3.txt","B")
				)
				
				//.aggregate(a -> a.processor(new AggregatorPojo()))
				//.log()
				.get();
	}
	
	@Bean
	public IntegrationFlow sampleFlowA() {
		return IntegrationFlows
				.from("channel-A")
				.transform(File.class,f -> {
					try {
						return FileUtils.readFileToString(f,"utf-8");
					} catch (IOException e) {
						return "";
					}
				})
				.log()
				.channel("channel-last")
				.get();
	}
	
	@Bean
	public IntegrationFlow sampleFlowB() {
		return IntegrationFlows
				.from("channel-B")
				.transform(File.class,f -> {
					try {
						return FileUtils.readFileToString(f,"utf-8");
					} catch (IOException e) {
						return "";
					}
				})
				.log()
				.channel("channel-last")
				.get();
	}
	
	@Bean
	public IntegrationFlow sampleFlowC() {
		return IntegrationFlows
				.from("channel-last")
				.aggregate(a -> a.processor(aggregator()))
				.log()
				.get();
	}
	
}
