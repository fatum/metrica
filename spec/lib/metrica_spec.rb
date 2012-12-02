require 'spec_helper'

describe Metrica do
  describe '#configure' do
    before do
      Metrica.configure do |config|
        config.storage = :cassandra
        config.options = { keyspace: 'test', servers: '5.9.90.15:9160' }
      end
    end

    its(:driver) { should be_instance_of(Metrica::Driver::Cassandra) }

    describe '#driver' do
      subject { Metrica.driver }

      its(:keyspace) { should == 'test' }
    end
  end
end
