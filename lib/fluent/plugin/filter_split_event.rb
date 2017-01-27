require 'fluent/plugin/filter'

module Fluent::Plugin
  class SplitEventFilter < Fluent::Plugin::Filter
    Fluent::Plugin.register_filter('split_event', self)

    desc 'Specify field name of record to split events on.'
    config_param :field, :string, default: 'message'
    desc 'Specify string to split on.'
    config_param :terminator, :string, default: ','

    def configure(conf)
      super
    end

    def filter_stream(tag, es)
      new_es = Fluent::MultiEventStream.new

      es.each do |time, record|
        begin
          if record.key?(@field)
            vals = record[@field].split(@terminator)

            if vals.count > 1
              vals.each do |v|
                new_record = record.dup
                new_record[@field] = v.strip

                new_es.add(time, new_record)
              end
            else
              new_es.add(time, record)
            end
          end
        rescue => e
          router.emit_error_event(tag, time, record, e)
        end
      end

      return new_es
    end
  end
end
