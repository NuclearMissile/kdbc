package kdbc

import java.math.BigDecimal
import java.sql.ResultSet
import java.sql.SQLException
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

@Suppress("UNCHECKED_CAST")
abstract class Table(private val name: String? = null) : ColumnOrTable {
    val columns = mutableListOf<Column<*>>()

    var tableAlias: String? = null
    var rs: ResultSet? = null
    val tableName: String get() = name ?: javaClass.simpleName.toLowerCase()

    inline protected fun <reified T : Any> column(name: String, ddl: String? = null, noinline getter: (ResultSet.(String) -> T)? = null): Column<T> {
        val column = Column(this, name, ddl, getter ?: DefaultGetter<T>()) {
            rs ?: throw SQLException("ResultSet was not configured when column value was requested")
        }

        columns += column
        return column
    }

    inline fun <reified T : Any> DefaultGetter(): ResultSet.(String) -> T = {
        when (T::class.javaPrimitiveType ?: T::class) {
            Int::class.javaPrimitiveType -> getInt(it) as T
            Long::class.javaPrimitiveType -> getLong(it) as T
            Float::class.javaPrimitiveType -> getFloat(it) as T
            Double::class.javaPrimitiveType -> getDouble(it) as T
            String::class -> getString(it) as T
            BigDecimal::class -> getBigDecimal(it) as T
            Date::class -> getDate(it) as T
            LocalDate::class -> getDate(it)?.toLocalDate() as T
            LocalDateTime::class -> getTimestamp(it)?.toLocalDateTime() as T
            else -> throw IllegalArgumentException("Default Column Getter cannot handle ${T::class} - supply a custom getter for this column")
        }
    }

    override fun toString() = if (tableAlias.isNullOrBlank() || tableAlias == tableName) tableName else "$tableName $tableAlias"

    // Generate DDL for this table. All columns that have a DDL statement will be included.
    fun ddl(dropIfExists: Boolean): String {
        val s = StringBuilder()
        if (dropIfExists) s.append("DROP TABLE IF EXISTS $tableName;\n")
        s.append("CREATE TABLE $tableName (\n")
        val ddls = columns.filter { it.ddl != null }.iterator()
        while (ddls.hasNext()) {
            val c = ddls.next()
            s.append("\t").append(c.name).append(" ").append(c.ddl)
            if (ddls.hasNext()) s.append(",\n")
        }
        s.append(")")
        return s.toString()
    }

    // Execute create table statement. Creates a query to be able to borrow a connection from the data source factory.
    fun create(dropIfExists: Boolean = false) {
        object : Query<Void>() {
            init {
                add(StringExpr(ddl(dropIfExists), this))
                execute()
            }
        }
    }
}

infix fun <T : Table> T.alias(alias: String): T {
    tableAlias = alias
    return this
}