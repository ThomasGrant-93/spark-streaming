# Spark Streaming

**Spark Streaming** — это проект для обработки и анализа потоковых данных из Kafka с использованием Apache Spark, с
последующим сохранением результатов в базе данных Cassandra. Он предоставляет возможности для выполнения аналитики в
реальном времени и разработки решений для обработки и агрегирования данных в режиме реального времени.

## Технологический стек

- **Apache Spark** — обработка данных в реальном времени.
- **Apache Kafka** — платформа для передачи потоковых данных.
- **Apache Cassandra** — база данных для хранения агрегированных результатов.
- **Scala** — язык программирования для написания приложений Spark.
- **Docker** — для изоляции и развертывания окружения.

## Основные компоненты
[docker-compose.yml](docker-compose.yml)
### Spark Kafka Consumer

`SparkKafkaConsumer` — основной класс для чтения данных из Kafka, их обработки с использованием оконных операций, а
также записи результатов в Cassandra.

#### Основные функции

1. **createSparkSession** — инициализация сессии Spark с подключением к Cassandra.
2. **createKafkaStream** — создание потока данных из Kafka с параметрами подключения и обработки.
3. **parseKafkaMessages** — обработка сообщений из Kafka с JSON-схемой `timestamp`, `device`, `temp`, `humd`, `pres`.
4. **aggregateData** — выполнение оконных операций для агрегации данных.
5. **run** — основная функция, запускающая потоковую обработку и запись данных в Cassandra.

#### Оконные операции

Метод `aggregateData` обрабатывает данные по 10-секундным окнам со смещением в 5 секунд, что позволяет выполнять
агрегацию данных по устройствам и времени.

### Docker Compose для Развертывания

Проект использует Docker Compose для запуска всех компонентов: Kafka, Zookeeper, Cassandra и Spark (главный узел и
несколько рабочих узлов). Файл `docker-compose.yml` предоставляет готовое окружение для тестирования и работы.

#### Основные сервисы

- **Zookeeper** — служит координатором для Kafka.
- **Kafka** — обработка и передача данных, автоматическое создание топиков включено.
- **Kafka-UI** — интерфейс для мониторинга и управления Kafka (`http://localhost:8082`).
- **Cassandra** — хранилище данных (порт `9042`).
- **Spark Master и Spark Workers** — узлы кластера для обработки данных.

### Пример данных

Пример JSON-сообщения, поступающего в `SparkKafkaConsumer`:

```json
{
  "timestamp": 1728366343.6998773,
  "device": "boston",
  "temp": 49.17,
  "humd": 50.74,
  "pres": 1021.39
}
```

## Сборка и запуск проекта

### Требования

- **Docker и Docker Compose**
- **sbt** — для сборки проекта на Scala.

### Сборка проекта

Для сборки проекта выполните:

```bash
sbt clean compile
```

### Запуск Docker Compose

Для запуска всех сервисов выполните:

```bash
docker-compose up -d
```

После запуска проверка доступности:

- **Spark Master** — [http://localhost:8080](http://localhost:8080)
- **Kafka-UI** — [http://localhost:8082](http://localhost:8082)

### Запуск `SparkKafkaConsumer`

Для запуска `SparkKafkaConsumer` используйте:

```bash
sbt "runMain SparkKafkaConsumer"
```

### Конфигурация

Файл `application.conf` должен содержать параметры подключения:

```hocon
app {
  name = "Spark"
  version = "1.0.0"
  spark {
    consumer {
      master = "local[*]"
      name = "Spark:Consumer"
      interval = "10 seconds"
    }
  }
  cassandra {
    host = "YOUR_CASSANDRA_HOST"
    port = "9042"
    keyspace = "stuff"
    table = "weather"
    user = "cassandra"
    pass = "cassandra"
  }
  database {
    host = "YOUR_DATABASE_HOST"
    port = "YOUR_DATABASE_PORT"
    db = "db"
    user = "edw_admin_user"
    pass = "edw_admin_user"
  }
  kafka {
    host = "YOUR_KAFKA_HOST"
    port = "9093"
    topic = "weather"
    checkpoint = "/tmp/kafka-checkpoints"
    offset = "earliest"
  }
}
```

## Логи и отладка

Для логирования и отладки используется Log4j. Настройки находятся в `log4j.properties`.
