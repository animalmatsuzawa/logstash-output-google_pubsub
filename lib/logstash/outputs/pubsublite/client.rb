require 'java'
require 'logstash-output-google_pubsub_jars.rb'
require 'logstash/outputs/pubsub/message_future_callback'

module LogStash
  module Outputs
    module PubsubLite
      # A wrapper around PubSub's Java API.
      class Client
        def initialize(json_key_file, topic_name, batch_settings, logger, client=nil)
          @logger = logger

          @pubsub = client || initialize_google_client(json_key_file, topic_name, batch_settings)
        end

        # Creates a Java BatchSettings object given user-defined thresholds.
        def self.build_batch_settings(byte_threshold, delay_threshold_secs, count_threshold)
          com.google.api.gax.batching.BatchingSettings.newBuilder
              .setElementCountThreshold(count_threshold)
              .setRequestByteThreshold(byte_threshold)
              .setDelayThreshold(org.threeten.bp.Duration.ofSeconds(delay_threshold_secs))
              .build
        end

        # Creates a Java PubsubMessage given the message body as a string and a
        # string:string hash of attributes
        def build_message(message_string, attributes)
          attributes ||= {}

          data = com.google.protobuf.ByteString.copyFromUtf8(message_string)
          builder = com.google.pubsub.v1.PubsubMessage.newBuilder
                       .setData(data)

          attributes.each { |k, v| builder.putAttributes(k, v) }

          builder.build
        end

        # Creates a PubsubMessage from the string and attributes
        # then queues it up to be sent.
        def publish_message(message_string, attributes)
          message = build_message(message_string, attributes)
          message_id = @pubsub.publish(message)
          setup_callback(message_string, message_id)
        end

        # Sets up the Google pubsub client.
        # It's unlikely this is needed out of initialize, but it's left public
        # for the purposes of mocking.
        def initialize_google_client(json_key_file, topic_name, batch_settings)
          @logger.info("Initializing Google API client on #{topic_name} key: #{json_key_file}")

          if use_default_credential? json_key_file
            credentials = com.google.cloud.pubsub.v1.TopicAdminSettings.defaultCredentialsProviderBuilder.build
          else
            raise_key_file_error(json_key_file)

            key_file = java.io.FileInputStream.new(json_key_file)
            sac = com.google.auth.oauth2.ServiceAccountCredentials.fromStream(key_file)
            credentials = com.google.api.gax.core.FixedCredentialsProvider.create(sac)
          end

          publisher_settings = com.google.cloud.pubsublite.cloudpubsub.PublisherSettings.newBuilder
            .setTopicPath(com.google.cloud.pubsublite.TopicPath.parse(topic_name))
            .setCredentialsProvider(credentials)
            .setBatchingSettings(batch_settings)
            .build

          publisher = com.google.cloud.pubsublite.cloudpubsub.Publisher.create(publisher_settings)
          publisher.startAsync.awaitRunning

          publisher
        end

        # Schedules immediate publishing of any outstanding messages and waits
        # until all are processed.
        def shutdown
          @pubsub.stopAsync.awaitTerminated
        end

        private

        def setup_callback(message_string, message_id)
          callback = LogStash::Outputs::Pubsub::MessageFutureCallback.new message_string, @logger

          com.google.api.core.ApiFutures.addCallback(
            message_id,
            callback,
            com.google.common.util.concurrent.MoreExecutors.directExecutor
          )
        end

        def construct_headers
          gem_name = 'logstash-output-google_pubsublite'
          user_agent = "Elastic/#{gem_name}"

          com.google.api.gax.rpc.FixedHeaderProvider.create('User-Agent', user_agent)
        end

        def use_default_credential?(key_file)
          key_file.nil? || key_file == ''
        end

        # raises an exception if the key file is invalid
        def raise_key_file_error(key_file)
          is_abs = ::File.absolute_path(key_file) == key_file
          raise "json_key_file must be an absolute path: #{key_file}" unless is_abs

          exists = ::File.exist? key_file
          raise "json_key_file does not exist: #{key_file}" unless exists
        end
      end
    end
  end
end
