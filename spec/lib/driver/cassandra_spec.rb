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
  end
end
