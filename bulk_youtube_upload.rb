require 'csv'
require 'open3'

module BulkYoutubeUpload
  class UploadException < Exception; end
  class QueueException < Exception; end

  class BulkYoutubeUpload
    def initialize(video_list:, log_output:)
      @videos_queue = UploadQueue.new(video_list)
      @logs = Logs.new(log_output)
    end

    def perform
      @logs.write('Initialized successfully')

      begin
        until @videos_queue.empty?
          video, metadata = @videos_queue.head
          self.upload(video, metadata)
          @videos_queue.pop
        end
      rescue Exception => e
        puts "ERROR: #{e}"
        @logs.write("ERROR: #{e}")
      ensure
        @videos_queue.save
        @logs.logfile.close
      end
    end

    def upload(video_path, metadata_path)
      @logs.write("Uploading #{video_path}")
      stdout, stderr, status = Open3.capture3("youtubeuploader -filename #{video_path} -metaJSON #{metadata_path}")

      raise UploadException, stderr.match(/\d{2}:\d{2}:\d{2}\s(.*)/)[1] unless stdout.match(/Upload successful/)
      @logs.write("Uploaded #{video_path} successfully")
    end
  end

  class Logs
    attr_accessor :logfile
    def initialize(logfile_path)
      @logfile = File.open(logfile_path, 'a')
    end

    def write(message)
      @logfile.puts("#{Time.now} | #{message}")
    end
  end

  class UploadQueue
    def initialize(queue_path)
      raise QueueException, "Could not open #{queue_path}" unless File.exists?(queue_path)
      @queue_path = queue_path
      @queue = CSV.parse(File.read(@queue_path))
    end

    def head
      @queue.first
    end

    def pop
      @queue.shift
    end

    def empty?
      @queue.empty?
    end

    def save
      CSV.open(@queue_path, 'w') do |csv|
        @queue.each do |row|
          csv << row
        end
      end 
    end

  end
end

@upload = BulkYoutubeUpload::BulkYoutubeUpload.new(video_list: 'list.csv', log_output: 'BYU.log')
@upload.perform