require 'spec_helper'
require 'metrica/driver/cassandra'

describe Metrica::Cassandra::Query::Offset do
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

  # new api Slicer.new(client).hour_offset('metric', now, offset)
  describe "#hour_offset" do
    subject { described_class.new('metric', rows).hour_offset(now, offset) }

    it { should_not be_empty }

    it 'should return valid slice' do
      subject.inject(&:+).should eq(4)
    end
  end
end
