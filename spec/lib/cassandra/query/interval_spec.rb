require 'spec_helper'
require 'metrica/driver/cassandra'

describe Metrica::Cassandra::Query::Interval do
  let!(:driver) do
    Metrica.configure do |config|
      config.storage = :cassandra
      config.options = { keyspace: 'test', servers: '5.9.90.15:9160' }
    end
  end
  let!(:client) { Metrica.driver.client }

  let(:cf) { Metrica::Driver::Cassandra::COLUMN_FAMILY }

  before { Metrica.driver.counter.increment('metric') }

  %w(minutes hours days).each do |time|
    context "#{time} range" do
      it "should change increment metric values" do
        query = described_class.new(client, cf)
        response = query.histogram('metric',
          Time.now, 5.minutes.ago, time.to_sym
        )

        response.count.should > 0

        response.keys.each do |k|
          k.should be_instance_of(Time)
        end

        response.values.each do |k|
          k.should > 0
        end
      end
    end
  end
end
