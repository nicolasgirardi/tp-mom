Algunos comentarios sobre lo hecho en factory.go

- Se consideró que si se llama a StartConsuming cuando ya está consumiendo, este último llamado termina sin hacer nada (no consume 2 veces)
- El mutex en MyQueueMiddleware y MyExchangeMiddleware está para evitar que se intente cancelar el consumo cuando está en proceso de empezar a consumir
- Como puede haber 2 gorutines intentando acceder al cosumerTag al mismo tiempo, también se lo protegió detrás de un mutex
- Close() también ejecuta un StopConsuming por si no se llama antes.
- Se entiende que cuando un exchange envía algo, lo envía a todas las keys que pasó al momento de crearse el exchange
- Para el send se podía hacer un publish o publishwithcontext. La diferencia entre estas es que publish with context hace un select por detras entre
un publish y el context que se le pasa. La idea de esto por lo que entendí es que el publish no se quede bloqueado indefinidamente y que
si ocurre algo antes (dependiendo del context: un timeout, deadline, un graceful shutdown, etc) se anule el publish. Se
decidió por publish a secas ya que no creo que pueda justificar un context para este trabajo y hacer publish sin context es igual de válido