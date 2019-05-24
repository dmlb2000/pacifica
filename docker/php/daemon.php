<?php
require_once __DIR__ . '/vendor/autoload.php';

function getenv_default($key, $default) {
    return getenv($key) ? getenv($key) : $default;
};

define('HOST', getenv_default('HOST', 'localhost'));
define('PORT', intval(getenv_default('PORT', '5672')));
define('USER', getenv_default('USER', 'guest'));
define('PASS', getenv_default('PASS', 'guest'));
define('VHOST', getenv_default('VHOST', '/'));
//If this is enabled you can see AMQP output on the CLI
#define('AMQP_DEBUG', true);

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Helper\MiscHelper;
use GuzzleHttp\Client;

$exchange = 'gov.pnnl.datahub.events';
$queue = 'org.pacifica.metadata.orm';
$consumerTag = 'drupal';

$connection = new AMQPStreamConnection(HOST, PORT, USER, PASS, VHOST);
$channel = $connection->channel();

/*
    The following code is the same both in the consumer and the producer.
    In this way we are sure we always have a queue to consume from and an
        exchange where to publish messages.
*/

/*
    name: $queue
    passive: false
    durable: true // the queue will survive server restarts
    exclusive: false // the queue can be accessed in other channels
    auto_delete: false //the queue won't be deleted once the channel is closed.
*/
$channel->queue_declare($queue, false, true, false, false);

/*
    name: $exchange
    type: direct
    passive: false
    durable: true // the exchange will survive server restarts
    auto_delete: false //the exchange won't be deleted once the channel is closed.
*/

$channel->exchange_declare($exchange, AMQPExchangeType::DIRECT, false, true, false);

$channel->queue_bind($queue, $exchange, $queue);

/**
 * @param \PhpAmqpLib\Message\AMQPMessage $message
 */
function process_message($message)
{
    echo "\n--------\n";
    echo $message->body;
    echo "\n--------\n";
    $table = $message->get('application_headers');
    echo MiscHelper::dump_table($table);
    echo "\n--------\n";
    $body_data = json_decode($message->body, true);
    foreach ($table as $name => $value) {
        if ($name === 'cloudEvents:source') {
            $client = new Client();
            foreach ($body_data['obj_primary_keys'] as $obj_hash) {
                $resp = $client->request('GET', $value[1], ['query' => $obj_hash]);
                assert($resp->getStatusCode() == 200);
                print(var_dump(json_decode($resp->getBody())));
            }
        }
    }
    echo "\n--------\n";

    $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
}

/*
    queue: Queue from where to get the messages
    consumer_tag: Consumer identifier
    no_local: Don't receive messages published by this consumer.
    no_ack: If set to true, automatic acknowledgement mode will be used by this consumer. See https://www.rabbitmq.com/confirms.html for details.
    exclusive: Request exclusive consumer access, meaning only this consumer can access the queue
    nowait:
    callback: A PHP Callback
*/

$channel->basic_consume($queue, $consumerTag, false, false, false, false, 'process_message');

/**
 * @param \PhpAmqpLib\Channel\AMQPChannel $channel
 * @param \PhpAmqpLib\Connection\AbstractConnection $connection
 */
function shutdown($channel, $connection)
{
    $channel->close();
    $connection->close();
}

register_shutdown_function('shutdown', $channel, $connection);

// Loop as long as the channel has callbacks registered
while ($channel ->is_consuming()) {
    $channel->wait();
}
