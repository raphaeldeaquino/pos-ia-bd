# Exportar variáveis da CLI AWS
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_KEY=
export AWS_SESSION_TOKEN=

# Iniciar connect sink
connect-standalone connect-standalone.properties s3.properties

# Inicializar o consumidor
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic s3_topic

# Iniciar connect source
connect-standalone connect-standalone.properties csv.properties

# Política para configurar AWS IoT Core Certificate
{ 
	"Version": "2012-10-17", 
	"Statement": [ 
		{ 
			"Effect": "Allow", 
			"Action": "iot:*", 
			"Resource": "*" 
		} 
	] 
}