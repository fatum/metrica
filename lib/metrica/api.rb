module Metrica
  module Api
    delegate :counter, :time, :timing, to: :driver

  private
    def driver
      self.class.driver
    end
  end
end
