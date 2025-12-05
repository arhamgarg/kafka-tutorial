#include <glib-2.0/glib.h>
#include <librdkafka/rdkafka.h>

#include "common.c"

static volatile sig_atomic_t run = 1;

static void stop(int sig) { run = 0; }

int main(int argc, char **argv) {
  rd_kafka_t *consumer;
  rd_kafka_conf_t *conf;
  rd_kafka_resp_err_t err;
  char errstr[512];

  conf = rd_kafka_conf_new();

  set_config(conf, "bootstrap.servers", "<BOOTSTRAP SERVERS>");
  set_config(conf, "sasl.username", "<CLUSTER API KEY>");
  set_config(conf, "sasl.password", "<CLUSTER API SECRET>");

  set_config(conf, "security.protocol", "SASL_SSL");
  set_config(conf, "sasl.mechanisms", "PLAIN");
  set_config(conf, "group.id", "kafka-tutorial");
  set_config(conf, "auto.offset.reset", "latest");

  consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));

  if (!consumer) {
    g_error("Failed to create new consumer: %s", errstr);
    return 1;
  }

  rd_kafka_poll_set_consumer(consumer);

  conf = NULL;

  const char *topic = "sample_data_stock_trades";
  rd_kafka_topic_partition_list_t *subscription =
      rd_kafka_topic_partition_list_new(1);

  rd_kafka_topic_partition_list_add(subscription, topic, RD_KAFKA_PARTITION_UA);

  err = rd_kafka_subscribe(consumer, subscription);

  if (err) {
    g_error("Failed to subscribe to %d topics: %s", subscription->cnt,
            rd_kafka_err2str(err));
    rd_kafka_topic_partition_list_destroy(subscription);
    rd_kafka_destroy(consumer);

    return 1;
  }

  rd_kafka_topic_partition_list_destroy(subscription);

  signal(SIGINT, stop);

  while (run) {
    rd_kafka_message_t *message = rd_kafka_consumer_poll(consumer, 500);

    if (!message) {
      g_message("Waiting...");
      continue;
    }

    if (message->err) {
      if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      } else {
        g_message("Consumer error: %s", rd_kafka_message_errstr(message));
        return 1;
      }
    } else {
      g_message("Consumed trade event from topic %s:\n%s",
                rd_kafka_topic_name(message->rkt), (char *)message->payload);
    }

    rd_kafka_message_destroy(message);
  }

  g_message("Closing consumer");
  rd_kafka_consumer_close(consumer);

  rd_kafka_destroy(consumer);

  return 0;
}
