package com.pinterest.doctork.servlet;

import com.pinterest.doctork.DoctorKMain;
import com.pinterest.doctork.OperatorAction;
import com.pinterest.doctork.config.DoctorKConfig;
import com.pinterest.doctork.util.OperatorUtil;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class DoctorKActionsServlet extends DoctorKServlet {

  private static final Logger LOG = LogManager.getLogger(DoctorKActionsServlet.class);
  private static final Gson gson = new Gson();
  private static final String OPERATOR_ACTIONS_CONSUMER_GROUP = "doctork_actions_consumer";
  private static final int NUM_MESSAGES = 1000;
  private static final long CONSUMER_POLL_TIMEOUT_MS = 1000L;
  private static final DecoderFactory avroDecoderFactory = DecoderFactory.get();
  private static Schema operatorActionSchema = OperatorAction.getClassSchema();
  private static SimpleDateFormat dtFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");

  @Override
  public void renderJSON(PrintWriter writer, Map<String, String> params) {
    JsonArray json = new JsonArray();

    for (ConsumerRecord<byte[], byte[]> record : Lists.reverse(retrieveActionReportMessages())) {
      try {
	JsonObject jsonRecord = new JsonObject();
	BinaryDecoder binaryDecoder = avroDecoderFactory.binaryDecoder(record.value(), null);
	SpecificDatumReader<OperatorAction> reader =
	  new SpecificDatumReader<>(operatorActionSchema);

	OperatorAction result = new OperatorAction();
	reader.read(result, binaryDecoder);

	jsonRecord.add("date",gson.toJsonTree(new Date(result.getTimestamp())));
	jsonRecord.add("clusterName",gson.toJsonTree(result.getClusterName()));
	jsonRecord.add("description",gson.toJsonTree(result.getDescription()));
	json.add(jsonRecord);
      } catch (Exception e) {
	LOG.info("Fail to decode an message", e);
      }
    }
    writer.print(json);
  }

  @Override
  public void renderHTML(PrintWriter writer, Map<String, String> params) {
    printHeader(writer);
    writer.print("<div> <p><a href=\"/\">Home</a> > doctork action </p> </div>");
    writer.print("<table class=\"table table-hover\"> ");
    writer.print("<th class=\"active\"> Timestamp </th> ");
    writer.print("<th class=\"active\"> Cluster </th> ");
    writer.print("<th class=\"active\"> Action </th>");

    try {
      for (ConsumerRecord<byte[], byte[]> record : Lists.reverse(retrieveActionReportMessages())) {
	try {
	  BinaryDecoder binaryDecoder = avroDecoderFactory.binaryDecoder(record.value(), null);
	  SpecificDatumReader<OperatorAction> reader =
            new SpecificDatumReader<>(operatorActionSchema);

	  OperatorAction result = new OperatorAction();
	  reader.read(result, binaryDecoder);

	  Date date = new Date(result.getTimestamp());
	  writer.println("<tr class=\"active\"> ");
	  writer.println("<td>" + dtFormat.format(date) + "</td>");
	  writer.println("<td>" + result.getClusterName() + "</td>");
	  writer.println("<td> " + result.getDescription() + "</td>");
	  writer.println("</tr>");
	} catch (Exception e) {
	  LOG.info("Fail to decode an message", e);
	}
      }
    } catch (Exception e) {
      LOG.error("Failed to get actions", e);
      e.printStackTrace(writer);
    }
    writer.print("</tbody> </table>");
    printFooter(writer);
  }


  private List<ConsumerRecord<byte[], byte[]>> retrieveActionReportMessages() {
    DoctorKConfig doctorKConfig = DoctorKMain.doctorK.getDoctorKConfig();
    String zkUrl = doctorKConfig.getBrokerstatsZkurl();
    String actionReportTopic = doctorKConfig.getActionReportTopic();
    Properties properties =
        OperatorUtil.createKafkaConsumerProperties(zkUrl, OPERATOR_ACTIONS_CONSUMER_GROUP,
            doctorKConfig.getActionReportProducerSecurityProtocol(),
            doctorKConfig.getActionReportProducerSslConfigs());
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);

    TopicPartition operatorReportTopicPartition = new TopicPartition(actionReportTopic, 0);
    List<TopicPartition> tps = new ArrayList<>();
    tps.add(operatorReportTopicPartition);
    consumer.assign(tps);

    Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(tps);
    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);
    for (TopicPartition tp : endOffsets.keySet()) {
      long numMessages = endOffsets.get(tp) - beginOffsets.get(tp);
      LOG.info("{} : offsets [{}, {}], num messages : {}",
          tp, beginOffsets.get(tp), endOffsets.get(tp), numMessages);
      consumer.seek(tp, Math.max(beginOffsets.get(tp), endOffsets.get(tp) - NUM_MESSAGES));
    }

    ConsumerRecords<byte[], byte[]> records = consumer.poll(CONSUMER_POLL_TIMEOUT_MS);
    List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();

    while (!records.isEmpty()) {
      for (ConsumerRecord<byte[], byte[]> record : records) {
        recordList.add(record);
      }
      records = consumer.poll(CONSUMER_POLL_TIMEOUT_MS);
    }
    LOG.info("Read {} messages", recordList.size());
    return recordList;
  }
}
