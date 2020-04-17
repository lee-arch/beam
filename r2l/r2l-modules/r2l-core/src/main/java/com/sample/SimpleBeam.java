package com.sample;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

public class SimpleBeam {
	public static void main1(String[] args) throws Exception{
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);
        // テキスト読み込み
        PCollection<String> textData = p.apply(TextIO.read().from(
        		new File(SimpleBeam.class.getClassLoader().getResource("Sample.txt").toURI()).getAbsolutePath()
        		));

        // テキスト書き込み
        textData
        	.apply(
        			MapElements
        				.into(TypeDescriptors.strings())
        				.via(s -> s + "/")
        		)
        	.apply(TextIO.write().to("c:\\temp\\wordcounts.txt"));
        // Pipeline 実行
        p.run().waitUntilFinish();

        System.exit(0);
    }


	public static void main2(String... args) throws Exception {
		args = new String[1];
		args[0] = "c:\\temp\\Sample.txt";

        PipelineOptions opt = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(opt);

        p.apply(TextIO.read().from(args[0]))
                .apply(Count.perElement()) // (1)
                .apply(ToString.kvs())     // (2)
                .apply(ParDo.of(new DoFn<String, String>() { // (3)
                    @ProcessElement
                    public void process(ProcessContext ctx) {
                        System.out.println(ctx.element());
                    }
                }));
        p.run().waitUntilFinish();
    }

	public static void main3(String...args) {
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);

		final List<String> arr = new ArrayList<>();
		for(int i = 1; i <= 100; i++) {
			arr.add("String"+i);
		}

		p.apply(GenerateSequence.from(0))
		.apply(ParDo.of(new DoFn<Long, String>(){
			@ProcessElement
			public void process(ProcessContext c) {
				//System.out.println(c.element());
				try {
					TimeUnit.MILLISECONDS.sleep(10);
				} catch (InterruptedException e) {
					// TODO 自動生成された catch ブロック
					e.printStackTrace();
				}
				c.output(arr.get((int)(c.element() % 100)));
			}
		}))
//		.apply(
//				Window.<String>into(Sessions.withGapDuration(Duration.standardSeconds(5)))
//				.triggering(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1))).discardingFiredPanes()
//			)
		.apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(3))))
		.apply(Count.perElement())
		.apply(ParDo.of(new DoFn<KV<String, Long>, String>(){
			@ProcessElement
			public void process(ProcessContext c) {
				System.out.println("-");
				System.out.println(c.element().getKey() + ":" + c.element().getValue());
			}
		}))
		;

		//p.run().waitUntilFinish();


		System.exit(0);
	}

	public static void main4(String[] args) {
	    String inputsDir = "c:\\temp\\*";
	    String outputsPrefix = "outputs/part";

	    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

	    Pipeline pipeline = Pipeline.create(options);
	    pipeline
	        .apply("Read lines", TextIO.read().from(inputsDir))
	        .apply("Find words", FlatMapElements.into(TypeDescriptors.strings())
	            .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
	        .apply("Filter empty words", Filter.by((String word) -> !word.isEmpty()))
	        .apply("Count words", Count.perElement())
	        .apply("Write results", MapElements.into(TypeDescriptors.strings())
	            .via((KV<String, Long> wordCount) ->
	                  wordCount.getKey() + ": " + wordCount.getValue()))
	        .apply(TextIO.write().to(outputsPrefix));
	    pipeline.run();

	}






	public static void main(String...args) {
//		args = new String[] {"--runner=DirectRunner"};
		PipelineOptions options = PipelineOptionsFactory.create();
//		options.setRunner(PipelineRunner.class);
		Pipeline p = Pipeline.create(options);

		final List<String> arr = new ArrayList<>();
		for(int i = 1; i <= 100; i++) {
			arr.add("String"+i);
		}

		p.apply(Create.of(arr))
		.apply(ParDo.of(new DoFn<String, String>(){
			@ProcessElement
			public void process(ProcessContext c) {
				System.out.println("------:" + Thread.currentThread().getId() + ":" + c.element());
			}
		}))
		;

		p.run().waitUntilFinish();


//		System.exit(0);
	}

}