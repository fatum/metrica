require 'spec_helper'
require 'metrica/driver/cassandra'

describe Metrica::Cassandra::Query::Offset do
  before do
    Metrica.configure do |config|
      config.storage = :cassandra
      config.options = { keyspace: 'test', servers: '5.9.90.15:9160' }
    end
  end

  describe 'hour offset' do
    let(:rows) {
      {
        # 2012-12-04 9:00
        'metric|2012120409' => {
          1 => 15,
          2 => 1,
          3 => 1,
          4 => 1
        },

        # 2012-12-04 10:01
        'metric|2012120410' => {
          1 => 1
        }
      }
    }

    let(:now) { Time.parse '2012-12-04 10:01' }
    let(:offset) { Time.parse '2012-12-04 9:01' }

    # new api Metrica::Query::Offset.new(client).hour('metric', now, offset)
    describe "#hour_offset" do
      before do
        Metrica::Cassandra::Query::Offset::Hour.any_instance.stub(:get_data => rows)
      end

      it 'should return valid slice' do
        described_class.new('metric').hour(now).should eq(4)
      end

      context "when empty" do
        let(:rows) do
          {
            # 2012-12-04 9:00
            'metric|2012120409' => {}
          }
        end

        before do
          Metrica::Cassandra::Query::Offset::Hour.any_instance.stub(:get_data => rows)
        end

        it 'should be 0' do
          described_class.new('metric').hour(now).should eq(0)
        end
      end
    end
  end

  describe 'minute' do
    let(:rows) {
      {
        # 2012-12-04 9:00
        'metric|2012120409' => {
          1 => 15,
          2 => 1,
          3 => 1,
          4 => 1
        },

        # 2012-12-04 10:01
        'metric|2012120410' => {
          1 => 1
        }
      }
    }

    let(:now) { Time.parse '2012-12-04 10:01' }
    let(:offset) { Time.parse '2012-12-04 9:01' }

    describe "#minute_offset" do
      before do
        Metrica::Cassandra::Query::Offset::Minute.any_instance.stub(:get_data => rows)
      end
      subject { described_class.new('metric').minute(offset) }

      it 'should return valid slice' do
        subject.should eq(15)
      end

      context "when empty" do
        let(:rows) do
          {
            # 2012-12-04 9:00
            'metric|2012120410' => {}
          }
        end

        before do
          Metrica::Cassandra::Query::Offset::Minute.any_instance.stub(:get_data => rows)
        end

        it 'should be 0' do
          described_class.new('metric').minute(now).should eq(0)
        end
      end
    end
  end

  describe 'day' do
    let(:rows) {
      {
        # DAY TIME OFFSET
        # day row 2012-12-03
        'metric|20121203' => {
          1 => 15,
          2 => 1, #match
          3 => 1, #match
          4 => 1 #match
        },
        # hour row 2012-12-03 1:02
        'metric|2012120301' => {
          1 => 1,
          2 => 2 #match
        },

        # CURRENT TIME
        # day row 2012-12-04
        'metric|20121204' => {
          1 => 1, #match
          2 => 2
        },
        # hour row 2012-12-03 1:02
        'metric|2012120401' => {
          1 => 1, # match
          2 => 2
        }
      }
    }

    let(:now) { Time.parse '2012-12-04 01:01' }
    describe "#day_offset" do
      it 'should return valid slice' do
        Metrica::Cassandra::Query::Offset::Day.any_instance.stub(:get_data => rows)
        described_class.new('metric').day(now).should eq(7)
      end

      describe 'integration' do
        let(:now) { 10.day.ago }

        before do
          Metrica.driver.counter.increment('metric', 1, now)
        end

        it 'should be not 0' do
          described_class.new('metric').day(now).should_not eq(0)
        end
      end

      context "when empty" do
        let(:rows) do
          {
            # 2012-12-04 9:00
            'metric|2012120401' => {}
          }
        end

        it 'should be 0' do
          Metrica::Cassandra::Query::Offset::Day.any_instance.stub(:get_data => rows)
          described_class.new('metric').day(now).should eq(0)
        end
      end
    end
  end
end
