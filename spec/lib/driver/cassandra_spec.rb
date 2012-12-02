require 'spec_helper'

describe Metrica::Driver::Cassandra do
  let(:driver) do
    Metrica.configure do |config|
      config.storage = :cassandra
      config.options = { keyspace: 'test', servers: '127.0.0.1:9161' }
    end

    Metrica.driver
  end

  describe "#counter" do
    context 'minute range' do
      before { driver.counter('metric') }

      it "should change increment metric values" do
        response = driver.histogram('metric',
          Time.now, 5.minutes.ago
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
