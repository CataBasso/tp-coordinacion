# Informe TP-COORDINACION 2C2026 - Catalina Basso - 108564

## Coordinacion

### Sum

Se pueden tener multiples replicas de `Sum` para cada cliente y asi reducir el tiempo de procesamiento. Cada instancia de `Sum` recibe mensajes de la cola de entrada del `Gateway`. Puede recibir dos tipos de mensaje:

1. Mensaje con datos, formado por: 
    - client id
    - la fruta
    - la cantidad de esa fruta

2. EOF: cuando el `Gateway` termina de enviar los datos de cada cliente, envia un mensaje de fin. Este mensaje solamente contiene el client id. 

Cuando una replica recibe un mensaje que solo contiene el client_id, entiende que ya no hay mas datos sobre ese cliente y notifica al resto de las instancias mediante un exchange de control. 

cada réplica de `Sum` recibe información a través de dos canales: una cola de datos compartida con el `Gateway` y una cola asociada al exchange de control, donde recibe los EOF enviados por las demás réplicas.

De esta manera, todas las instancias de `Sum` reciben una notificación de que ese cliente terminó. Al recibirla, cada instancia realiza un `flush` de los datos que acumuló para ese cliente. Para evitar que esta accion se realice mientras una instancia todavía está procesando un mensaje de datos del mismo cliente, se utiliza un `Lock` junto con una `Condition` en vez de ejecutar `flush` sin cheaquear nada antes. El `Lock` protege el acceso concurrente a los diccionarios que guardan los datos, mientras que la `Condition` permite que el procesamiento del `EOF` espere hasta que termine el procesamiento del dato que se encuentra actualmente en curso. Osea, cuando se recibe un EOF de otra replica, existe una especie de barrera que primero espera a que si hay un mensaje de ese mismo client_id procesandose, termine y luego se envien los datos acumulados a `Aggregation`.   

`flush` calcula qué instancia de `Aggregation` debe recibir cada fruta (se hace una especie de hashing para que misma fruta vaya a misma replica de `Aggregation` y asi terminar de unificar cantidades) y envía los resultados parciales correspondientes.

Además, las colas del middleware utilizan `prefetch_count=1`, por lo que cada consumidor tiene como máximo un mensaje entregado y todavía no confirmado mediante `ACK`. Esto evita que una misma réplica tenga varios mensajes de datos pendientes de procesamiento al mismo tiempo. Junto con el uso de `Lock` y `Condition`, permite controlar la situación en la que una réplica recibe la notificación de fin de un cliente mientras todavía está procesando un mensaje de datos de ese mismo cliente. En ese caso, el procesamiento del `EOF` espera hasta que termine el procesamiento que se encuentra en curso antes de realizar el `flush`. 

Una vez finalizado el procesamiento del cliente, cada réplica de `Sum` envía un mensaje de fin a las instancias de `Aggregation`. El mensaje contiene el `client_id` y el identificador de la réplica de `Sum` (`sum_id`), de forma que `Aggregation` pueda saber qué réplica terminó de enviar sus datos.

### Aggregation

Las instancias de `Aggregation` reciben los resultados parciales producidos por las réplicas de `Sum`.

Cada mensaje de datos contiene:

* client_id
* la fruta
* la cantidad acumulada para esa fruta
* sum_id

Las instancias de `Aggregation` acumulan estos datos por cliente y por fruta. Como distintas réplicas de `Sum` pueden haber procesado partes diferentes de los datos de un mismo cliente, una misma fruta puede llegar desde distintas réplicas de `Sum`. Sin embargo, mediante la partición determinística realizada por `Sum`, todos los mensajes correspondientes a una misma fruta son enviados a la misma réplica de `Aggregation`. De esta forma, `Aggregation` puede combinar las cantidades de esa fruta provenientes de las distintas réplicas de `Sum`.

Además, cada instancia de `Aggregation` necesita esperar a que todas las réplicas de `Sum` hayan terminado antes de generar su resultado parcial. Para esto, cada instancia mantiene, para cada cliente, el conjunto de identificadores de las réplicas de `Sum` que ya enviaron su mensaje de fin. El resultado parcial de un cliente solamente se genera cuando se recibieron los EOF correspondientes a todas las réplicas de `Sum`.

Una vez finalizado este proceso, `Aggregation` calcula el top correspondiente a las frutas de su propia partición y envía ese resultado parcial hacia `Join`.

### Flujo y union con `Join`

Como existen múltiples réplicas de `Aggregation`, cada una procesa una parte de las frutas. Para repartirlas, `Sum` utiliza una función determinística que, a partir del nombre de la fruta, obtiene un índice entre `0` y `AGGREGATION_AMOUNT - 1`.

De esta forma, una determinada fruta siempre es enviada a la misma réplica de `Aggregation`. Esto permite distribuir el trabajo entre las réplicas sin necesidad de que todas procesen todos los datos.

A diferencia de los mensajes de datos, los mensajes de EOF de `Sum` se envían a todas las réplicas de `Aggregation`. Esto permite que cada réplica pueda saber cuándo terminaron todas las instancias de `Sum`, incluso aunque esa réplica no haya recibido datos de todas las frutas.

Cuando una réplica de `Aggregation` recibe todos los EOF de `Sum` para un cliente, genera su `top` parcial y lo envía a `Join`.

`Join` recibe los resultados parciales de las diferentes réplicas de `Aggregation` y cuenta cuántos recibió para cada cliente. Cuando recibe `AGGREGATION_AMOUNT` resultados parciales, los combina y calcula el `top` final. De esta manera, `Join` actúa como punto de reunión de los resultados producidos por las distintas réplicas de `Aggregation`.

## Escalabilidad

### Escalabilidad respecto de la cantidad de clientes

El sistema puede procesar múltiples clientes de manera concurrente. Los mensajes internos contienen el `client_id`, por lo que los datos de diferentes clientes pueden coexistir en las estructuras internas de `Sum` y `Aggregation` sin confundirse entre sí.

En `Sum`, los datos se mantienen separados por cliente. `Aggregation` mantiene sus resultados parciales separados por `client_id`, y `Join` utiliza el mismo identificador para reunir los resultados correspondientes a cada cliente.

Esto permite que mientras se procesan datos de un cliente, también puedan estar llegando mensajes correspondientes a otros clientes y no mezclar los datos de ellos. No es necesario que el sistema termine completamente con un cliente para comenzar a recibir datos de otro.

El `client_id` también permite que el `Gateway` identifique a qué conexión corresponde cada resultado final. Para esto, se agrego al `MessageHandler` el id del cliente en los mensajes internos y de esta manera filtra los resultados recibidos para que cada cliente reciba únicamente su propio resultado.

## Escalabilidad frente a grandes volúmenes de datos

El procesamiento se realiza de manera incremental. Las instancias de `Sum` no necesitan conservar todos los registros originales recibidos de los clientes, sino que van acumulando las cantidades por fruta.

Entonces, si un replica de `Sum` recibe muchos registros de una misma fruta para un mismo cliente, los registros se van actualizando (sumando a la cantidad de esa fruta ya almacenada) en vez de crear una nuevo `FruitItem`. De esta manera, la cantidad de información almacenada en memoria depende principalmente de la cantidad de clientes y frutas diferentes que se estén procesando, y no directamente de la cantidad total de registros recibidos.

Además, el trabajo puede distribuirse entre múltiples réplicas. Al aumentar la cantidad de instancias de `Sum`, los mensajes de entrada pueden ser procesados concurrentemente por distintas réplicas. Al aumentar la cantidad de instancias de `Aggregation`, las frutas pueden distribuirse entre ellas mediante la partición determinística utilizada por `Sum`.

Esto permite aumentar el paralelismo del procesamiento cuando crece el volumen de datos.

## Escalabilidad respecto de la cantidad de controles

La cantidad de instancias de `Sum` y `Aggregation` se configura mediante las variables de entorno `SUM_AMOUNT` y `AGGREGATION_AMOUNT`. El sistema no depende de una cantidad fija de réplicas, sino que utiliza estos valores para adaptar la comunicación entre las instancias.

Cada instancia tiene un identificador que permite distinguirla de las demás. A partir de este identificador y de la cantidad total de instancias configuradas, se crean los canales de comunicación necesarios.

Cuando aumenta `SUM_AMOUNT`, existen más réplicas de `Sum` procesando los mensajes de entrada. Cada `Aggregation` recibe los mensajes de fin enviados por todas las réplicas de `Sum` y utiliza los identificadores sum_id para saber cuándo todas terminaron de procesar los datos de un cliente.

Cuando aumenta `AGGREGATION_AMOUNT`, las frutas se distribuyen entre una mayor cantidad de instancias de `Aggregation`. La partición se realiza de forma determinística a partir del nombre de la fruta, por lo que una misma fruta siempre es enviada a la misma instancia. De esta manera, las distintas réplicas de `Sum` pueden enviar cantidades parciales de una misma fruta a una única `Aggregation`, donde son combinadas.

Finalmente, `Join` espera un resultado parcial de cada instancia de `Aggregation`. Cuando recibe `AGGREGATION_AMOUNT` resultados para un cliente, los reúne y calcula el top final.

Así, la coordinación del sistema se adapta a la cantidad de réplicas configurada sin necesidad de modificar la comunicación externa con los clientes. Esto permite utilizar distintas cantidades de instancias según el escenario de ejecución y distribuir el procesamiento entre ellas.

## Resumen

La coordinación se basa en mensajes internos y en identificadores de clientes y réplicas. `Sum` recibe y acumula los datos, coordina el fin del procesamiento entre sus réplicas y envía los resultados parciales a `Aggregation`. `Aggregation` espera a que todas las réplicas de `Sum` terminen, combina los datos correspondientes y genera un `top` parcial. Finalmente, `Join` reúne los resultados de todas las réplicas de `Aggregation` y produce el resultado final para cada cliente.

La arquitectura permite escalar tanto en cantidad de clientes como en cantidad de datos y de réplicas. La utilización de colas, procesamiento concurrente, acumulación incremental, particionamiento de las frutas y mensajes de finalización permite distribuir el trabajo entre las distintas instancias manteniendo separados los datos correspondientes a cada cliente.

