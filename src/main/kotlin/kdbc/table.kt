package kdbc

import java.math.BigDecimal
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*

@Suppress("UNCHECKED_CAST")
abstract class Table(private val name: String? = null) : ColumnOrTable {
    val columns = mutableListOf<Column<*>>()

    var tableAlias: String? = null
    var rs: ResultSet? = null
    val tableName: String get() = name ?: javaClass.simpleName.lowercase()

    protected inline fun <reified T : Any> column(name: String, ddl: String? = null, noinline getter: (ResultSet.(String) -> T)? = null): Column<T> {
        val column = Column(this, name, ddl, getter ?: defaultGetter()) {
            rs ?: throw SQLException("ResultSet was not configured when column value was requested")
        }

        columns += column
        return column
    }

    protected inline fun <reified T : Any> defaultGetter(): ResultSet.(String) -> T? = {
        when (T::class.javaPrimitiveType ?: T::class) {
            UUID::class -> getUUID(it) as? T
            Int::class.javaPrimitiveType -> getInt(it) as? T
            Long::class.javaPrimitiveType -> getLong(it) as? T
            Float::class.javaPrimitiveType -> getFloat(it) as? T
            Double::class.javaPrimitiveType -> getDouble(it) as? T
            String::class -> getString(it) as? T
            Boolean::class -> getBoolean(it) as? T
            BigDecimal::class -> getBigDecimal(it) as? T
            Date::class -> getDate(it) as? T
            LocalTime::class -> getLocalTime(it) as? T
            LocalDate::class -> getLocalDate(it) as? T
            LocalDateTime::class -> getLocalDateTime(it) as? T
            else -> throw IllegalArgumentException("Default Column Getter cannot handle ${T::class} - supply a custom getter for this column")
        }
    }

    override fun toString() = if (tableAlias.isNullOrBlank() || tableAlias == tableName) tableName else "$tableName $tableAlias"

    // Generate DDL for this table. All columns that have a DDL statement will be included.
    internal fun ddl(skipIfExists: Boolean, dropIfExists: Boolean): String {
        return with (StringBuilder())
        {
            if (dropIfExists) append("DROP TABLE IF EXISTS $tableName;\n")

            if (skipIfExists) append("CREATE TABLE IF NOT EXISTS $tableName (\n")
            else append("CREATE TABLE $tableName (\n")

            val ddls = columns.filter { it.ddl != null }.iterator()
            while (ddls.hasNext()) {
                val c = ddls.next()
                append("\t").append(c.name).append(" ").append(c.ddl)
                if (ddls.hasNext()) append(",\n")
            }

            append(")")
        }.toString()
    }

    // Execute create table statement. Creates a query to be able to borrow a connection from the data source factory.
    fun create(connection: Connection? = null, skipIfExists: Boolean = false, dropIfExists: Boolean = false) {
        object : Query<Void>(connection) {
            init {
                add(StringExpr(ddl(skipIfExists, dropIfExists), this))
                execute()
            }
        }
    }
}

infix fun <T : Table> T.alias(alias: String): T {
    tableAlias = alias
    return this
}