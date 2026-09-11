Algunos comentarios sobre lo hecho en factory.go

- Se consideró que si se llama a StartConsuming cuando ya está consumiendo, este último llamado termina sin hacer nada (no consume 2 veces)
- El mutex en MyQueueMiddleware y MyExchangeMiddleware está para evitar que se intente cancelar el consumo cuando está en proceso de empezar a consumir
- Como puede haber 2 gorutines intentando acceder al cosumerTag al mismo tiempo, también se lo protegió detrás de un mutex
- 