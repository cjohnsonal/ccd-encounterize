require 'mongo'
require 'json'

Mongo::Logger.logger.level = ::Logger::FATAL

client = Mongo::Client.new(['127.0.0.1:27017'], :database => 'pophealth-development')

encounterFile = "./encounter-base.json"
encounterBase = File.read(encounterFile)
superfile = Array.new
log = ""

records = client[:records].find({"encounters" => {"$exists" => false}})
recordarr = records.to_a.to_json

r = JSON.parse(recordarr)

for record in r
        return if record['encounters'] != nil
	encounter = JSON.parse(encounterBase)
	encounterTime = 0

	if(record['vital_signs'] != nil && record['vital_signs'][0]['time'] > 0)
		encounterTime = record['vital_signs'][0]['time']
	elsif(record['conditions'] != nil && record['conditions'][0]['start_time'] != nil && record['conditions'][0]['start_time'] > 0)
		encounterTime = record['conditions'][0]['start_time']
	elsif(record['provider_performances'] != nil && record['provider_performances'][0]['start_date'] > 0)
		encounterTime = record['provider_performances'][0]['start_date']
	end

	puts "Please check record #{record['medical_record_number']} for errors!" if encounterTime < 100

	encounterTime = ((encounterTime * 0.001).floor * 1000) + 777

	encounter['start_time'] = encounter['end_time'] = encounter['dischargeTime'] = encounterTime
	record['encounters'] = Array.new
	record['encounters'] << encounter
	superfile << record

	log += JSON.pretty_generate(encounter)
end

File.open("./log.json",'a'){|f| f << log}
File.open("./super.json",'w'){|f| f << JSON.pretty_generate(superfile)}

result = client[:records].delete_many({"encounters" => {"$exists" => false}})

system("mongoimport --host 127.0.0.1:27017 --db pophealth-development --collection records --file super.json --jsonArray")
