package kdbc

import java.io.InputStream
import java.math.BigDecimal
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*
import javax.sql.DataSource
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

fun Connection.execute(sql: String) = prepareStatement(sql).execute()
fun DataSource.execute(sql: String, autoclose: Boolean = true): Boolean {
    return try {
        connection.execute(sql)
    } finally {
        if (autoclose) connection.close()
    }
}

fun DataSource.select(sql: String, vararg v: Any) = connection.prepareStatement(sql)
    .let {
        it.processParameters(v)
        it.executeQuery()
    }

fun PreparedStatement.processParameters(v: Array<out Any>) = v.forEachIndexed { pos, param ->
    when (param) {
        is UUID -> setObject(pos + 1, param)
        is Int -> setInt(pos + 1, param)
        is Long -> setLong(pos + 1, param)
        is Float -> setFloat(pos + 1, param)
        is Double -> setDouble(pos + 1, param)
        is String -> setString(pos + 1, param)
        is BigDecimal -> setBigDecimal(pos + 1, param)
        is Boolean -> setBoolean(pos + 1, param)
        is LocalTime -> setTime(pos + 1, java.sql.Time.valueOf(param))
        is LocalDate -> setDate(pos + 1, java.sql.Date.valueOf(param))
        is LocalDateTime -> setTimestamp(pos + 1, java.sql.Timestamp.valueOf(param))
        is InputStream -> setBinaryStream(pos + 1, param)
        is Enum<*> -> setObject(pos + 1, param)
    }
}

class ConnectionFactory {
    companion object {
        internal val transactionContext = ThreadLocal<TransactionContext>()
        internal val isTransactionActive: Boolean
            get() = transactionContext.get() != null
    }

    internal var factoryFn: (Query<*>) -> Connection = {
        throw SQLException("No default data source is configured. Use Query.db() or configure `KDBC.dataSourceProvider.\n${it.describe()}")
    }

    internal fun borrow(query: Query<*>): Connection {
        val activeTransaction = transactionContext.get()
        if (activeTransaction?.connection != null)
            return activeTransaction.connection!!
        val connection = factoryFn(query)
        activeTransaction?.trackConnection(connection)
        return connection
    }
}

fun <T : Table> Connection.createTable(
    tableClass: KClass<T>,
    skipIfExists: Boolean = false,
    dropIfExists: Boolean = false
) = execute(tableClass.primaryConstructor!!.call().ddl(skipIfExists, dropIfExists))

