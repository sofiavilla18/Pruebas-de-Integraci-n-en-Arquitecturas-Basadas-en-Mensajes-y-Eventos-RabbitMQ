
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Notification.Worker;

public class Worker : BackgroundService
{
    private const string QueueName = "pedidos";

    private readonly ILogger<Worker> _logger;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST")
                       ?? "localhost",

            UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER")
                       ?? "admin",

            Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD")
                       ?? "admin"
        };

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _logger.LogInformation(
                    "Conectando con RabbitMQ..."
                );

                await using var connection =
                    await factory.CreateConnectionAsync(stoppingToken);

                await using var channel =
                    await connection.CreateChannelAsync(
                        cancellationToken: stoppingToken
                    );

                // Declarar la misma cola que utiliza la API.
                await channel.QueueDeclareAsync(
                    queue: QueueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null,
                    cancellationToken: stoppingToken
                );

                // Procesar un mensaje a la vez por consumidor.
                await channel.BasicQosAsync(
                    prefetchSize: 0,
                    prefetchCount: 1,
                    global: false,
                    cancellationToken: stoppingToken
                );

                var consumer = new AsyncEventingBasicConsumer(channel);

                consumer.ReceivedAsync += async (_, eventArgs) =>
                {
                    var body = eventArgs.Body.ToArray();

                    try
                    {
                        using var document = JsonDocument.Parse(body);
                        var order = document.RootElement;

                        string orderId = GetString(order, "orderId");
                        string customer = GetString(order, "customerName");
                        string product = GetString(order, "product");

                        int quantity = order.TryGetProperty(
                            "quantity", out var quantityElement)
                            ? quantityElement.GetInt32()
                            : 0;

                        _logger.LogInformation(
                            "Pedido recibido: {OrderId}",
                            orderId
                        );

                        _logger.LogInformation(
                            "Cliente: {Customer} | Producto: {Product} | Cantidad: {Quantity}",
                            customer,
                            product,
                            quantity
                        );

                        // Simular procesamiento entre 2 y 5 segundos.
                        int seconds = Random.Shared.Next(2, 6);

                        _logger.LogInformation(
                            "Procesando pedido {OrderId} durante {Seconds} segundos...",
                            orderId,
                            seconds
                        );

                        await Task.Delay(
                            TimeSpan.FromSeconds(seconds),
                            stoppingToken
                        );

                        // Confirmar el mensaje después de procesarlo.
                        await channel.BasicAckAsync(
                            deliveryTag: eventArgs.DeliveryTag,
                            multiple: false,
                            cancellationToken: stoppingToken
                        );

                        _logger.LogInformation(
                            "Pedido {OrderId} procesado y confirmado (ACK).",
                            orderId
                        );
                    }
                    catch (OperationCanceledException)
                        when (stoppingToken.IsCancellationRequested)
                    {
                        // El servicio se está deteniendo.
                    }
                    catch (JsonException ex)
                    {
                        _logger.LogError(
                            ex,
                            "Mensaje JSON inválido. Se rechazará sin reencolar."
                        );

                        await channel.BasicNackAsync(
                            deliveryTag: eventArgs.DeliveryTag,
                            multiple: false,
                            requeue: false,
                            cancellationToken: stoppingToken
                        );
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "Error procesando el mensaje. Se reencolará."
                        );

                        if (!stoppingToken.IsCancellationRequested)
                        {
                            await channel.BasicNackAsync(
                                deliveryTag: eventArgs.DeliveryTag,
                                multiple: false,
                                requeue: true,
                                cancellationToken: stoppingToken
                            );
                        }
                    }
                };

                await channel.BasicConsumeAsync(
                    queue: QueueName,
                    autoAck: false,
                    consumer: consumer,
                    cancellationToken: stoppingToken
                );

                _logger.LogInformation(
                    "Worker conectado. Escuchando la cola {QueueName}.",
                    QueueName
                );

                // Mantener activa la conexión mientras el servicio funciona.
                await Task.Delay(
                    Timeout.InfiniteTimeSpan,
                    stoppingToken
                );
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "No fue posible mantener la conexión con RabbitMQ. Se intentará de nuevo en 5 segundos."
                );

                try
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(5),
                        stoppingToken
                    );
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }

        _logger.LogInformation("Worker detenido.");
    }

    private static string GetString(
        JsonElement element,
        string propertyName)
    {
        return element.TryGetProperty(propertyName, out var value)
            ? value.ToString()
            : "No especificado";
    }
}
