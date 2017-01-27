require 'helper'

require 'timecop'
require 'fluent/test/driver/filter'

class SplitEventFilterTest < Test::Unit::TestCase

  setup do
    @tag = 'test.tag'
    @tag_parts = @tag.split('.')
    @time = event_time('2010-05-04 03:02:01 UTC')
    Timecop.freeze(@time)
  end

  teardown do
    Timecop.return
  end

  def create_driver(conf = '')
    Fluent::Test::Driver::Filter.new(Fluent::Plugin::SplitEventFilter).configure(conf)
  end

  sub_test_case 'configure' do
    test 'check default' do
      assert_nothing_raised do
        create_driver
      end
    end
  end

  sub_test_case "test default" do
    def filter(config, msgs = [''])
      d = create_driver(config)
      d.run {
        msgs.each { |msg|
          d.feed(@tag, @time, {'foo' => 'bar', 'message' => msg})
        }
      }
      d.filtered
    end

    test 'typical usage' do
      msgs = ['1', '2', '3', '4,5,6']

      filtered = filter('', msgs)

      all = []
      msgs.each do |m|
        m.split(',').each { |e| all << e.strip }
      end

      assert_equal(all.count, filtered.count)

      filtered.each_with_index do |(_t, r), i|
        assert_equal(@time, _t)
        assert_equal('bar', r['foo'])
        assert_equal(all[i], r['message'])
      end
    end
  end

  sub_test_case "test default" do
    def filter(config, msgs = [''])
      d = create_driver(config)
      d.run {
        msgs.each { |msg|
          d.feed(@tag, @time, {'foo' => 'bar', 'data' => msg})
        }
      }
      d.filtered
    end

    FIELDCONFIG = %[
      field data
    ]

    test 'typical usage' do
      msgs = ['1', '2', '3', '4,5,6']

      filtered = filter(FIELDCONFIG, msgs)

      all = []
      msgs.each do |m|
        m.split(',').each { |e| all << e.strip }
      end

      assert_equal(all.count, filtered.count)

      filtered.each_with_index do |(_t, r), i|
        assert_equal(@time, _t)
        assert_equal('bar', r['foo'])
        assert_equal(all[i], r['data'])
      end
    end
  end

  sub_test_case "test terminator" do
    def filter(config, msgs = [''])
      d = create_driver(config)
      d.run {
        msgs.each { |msg|
          d.feed(@tag, @time, {'foo' => 'bar', 'data' => msg})
        }
      }
      d.filtered
    end

    TERMINATORCONFIG = %[
      field      data
      terminator |
    ]

    test 'typical usage' do
      msgs = ['1', '2', '3', '4|5|6']

      filtered = filter(TERMINATORCONFIG, msgs)

      all = []
      msgs.each do |m|
        m.split('|').each { |e| all << e.strip }
      end

      assert_equal(all.count, filtered.count)

      filtered.each_with_index do |(_t, r), i|
        assert_equal(@time, _t)
        assert_equal('bar', r['foo'])
        assert_equal(all[i], r['data'])
      end
    end
  end
end
