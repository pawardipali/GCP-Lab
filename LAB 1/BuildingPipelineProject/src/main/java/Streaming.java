import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Streaming {
    public static void main(String[] args) {
        DataflowPipelineOptions dataflowPipelineOptions= PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        dataflowPipelineOptions.setJobName("StreamingIngestion");
        dataflowPipelineOptions.setProject("qwiklabs-gcp-03-1e261850a8ae");
        dataflowPipelineOptions.setRegion("australia-southeast1");
        dataflowPipelineOptions.setGcpTempLocation("gs://tmp123121");
        dataflowPipelineOptions.setRunner(DataflowRunner.class);

        Pipeline pipeline= Pipeline.create(dataflowPipelineOptions);

        PCollection<String> pubsubmessage=pipeline.apply(PubsubIO.readStrings().fromTopic("projects/qwiklabs-gcp-03-1e261850a8ae/topics/LabITDemo"));
        PCollection<TableRow> bqrow=pubsubmessage.apply(ParDo.of(new ConvertorStringBq()));

        bqrow.apply(BigQueryIO.writeTableRows().to("qwiklabs-gcp-03-1e261850a8ae:smalltech.pubsubStream")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run();

    }
   private static class ConvertorStringBq extends DoFn<String,TableRow> {
        public void processing(ProcessContext processContext){
            TableRow tableRow=new TableRow().set("message",processContext.element().toString())
                    .set("messageid",processContext.element().toString()+":"+processContext.timestamp().toString())
                    .set("messageprocessingtime",processContext.timestamp().toString());
            processContext.output(tableRow);

        }

   }

}
