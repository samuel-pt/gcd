package com.palmtree;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.FlatMapElements;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sam on 31/1/17.
 */
@SuppressWarnings("serial")
public class AllRides {
    private static final Logger LOG = LoggerFactory.getLogger(AllRides.class);

    // ride format from PubSub
    // {
    // "ride_id":"a60ba4d8-1501-4b5b-93ee-b7864304d0e0",
    // "latitude":40.66684000000033,
    // "longitude":-73.83933000000202,
    // "timestamp":"2016-08-31T11:04:02.025396463-04:00",
    // "meter_reading":14.270274,
    // "meter_increment":0.019336415,
    // "ride_status":"enroute" / "pickup" / "dropoff"
    // "passenger_count":2
    // }

    private static class PassThroughAllRides extends DoFn<TableRow, TableRow> {
        PassThroughAllRides() {}

        @Override
        public void processElement(ProcessContext c) {
            TableRow ride = c.element();

            // Access to data fields:
            // float lat = Float.parseFloat(ride.get("latitude").toString());
            // float lon = Float.parseFloat(ride.get("longitude").toString());

            c.output(ride);
        }
    }

    public static void main(String[] args) {
        CustomPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
        Pipeline p = Pipeline.create(options);

        p.apply(PubsubIO.Read.named("read from PubSub")
                .topic(String.format("projects/%s/topics/%s", options.getSourceProject(), options.getSourceTopic()))
                .timestampLabel("ts")
                .withCoder(TableRowJsonCoder.of()))

                // A Parallel Do (ParDo) transforms data elements one by one.
                // It can output zero, one or more elements per input element.
                .apply("pass all rides through 1", ParDo.of(new PassThroughAllRides()))

                // In Java 8 you can also use a simpler syntax through MapElements.
                // MapElements allows a single output element per input element.
                .apply("pass all rides through 2",
                        MapElements.via((TableRow e) -> e).withOutputType(TypeDescriptor.of(TableRow.class)))

                // In java 8, if you need to return zero one or more elements per input, you can use
                // the FlatMapElements syntax. It expects you to return an iterable and will
                // gather all of its values into the output PCollection.
                .apply("pass all rides through 3",
                        FlatMapElements.via(
                                (TableRow e) -> {
                                    List<TableRow> a = new ArrayList<>();
                                    a.add(e);
                                    return a;
                                })
                                .withOutputType(TypeDescriptor.of(TableRow.class)))

                .apply(PubsubIO.Write.named("write to PubSub")
                        .topic(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getSinkTopic()))
                        .withCoder(TableRowJsonCoder.of()));
        p.run();
    }
}
