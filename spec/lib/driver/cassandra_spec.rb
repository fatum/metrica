require 'spec_helper'

describe Metrica::Driver::Cassandra do
  let(:driver) do
    Metrica.configure do |config|
      config.storage = :cassandra
      config.options = { keyspace: 'test', servers: '5.9.90.15:9160' }
    end

    Metrica.driver
  end

  describe "#counter" do
    before { driver.counter.increment('metric') }

    %w(minutes hours days).each do |time|
      context "#{time} range" do
        it "should change increment metric values" do
          response = driver.counter.histogram('metric',
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
end
