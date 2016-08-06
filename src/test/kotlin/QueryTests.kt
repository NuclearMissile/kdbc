import kdbc.execute
import kdbc.query
import kdbc.update
import org.h2.jdbcx.JdbcDataSource
import org.junit.Assert.assertEquals
import org.junit.Test
import java.sql.ResultSet
import java.sql.SQLException
import javax.sql.DataSource

data class Customer(var id: Int, var name: String) {
    constructor() : this(0, "")
    constructor(rs: ResultSet) : this(rs.getInt("id"), rs.getString("name"))
}

class QueryTests {
    companion object {
        private val db: DataSource

        init {
            db = JdbcDataSource().apply {
                setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
                execute { "CREATE TABLE customers (id integer not null primary key, name text)" }
                execute { "INSERT INTO customers VALUES (1, 'John')" }
                execute { "INSERT INTO customers VALUES (2, 'Jill')" }
            }
        }
    }

    fun getCustomerById(id: Int): Customer = db.query {
        "SELECT * FROM customers WHERE id = ${p(id)}"
    } single {
        Customer(getInt("id"), getString("name"))
    }

    @Test
    fun queryTest() {
        val john = getCustomerById(1)
        assertEquals(1, john.id)
        assertEquals("John", john.name)
    }

    @Test
    fun updateTest() {
        val id = 1
        val name = "Johnnie"

        val updateCount = db.update {
            """UPDATE customers SET
            name = ${name.q}
            WHERE id = ${id.q}
            """
        }
        assertEquals(1, updateCount)

        val updatedName = getCustomerById(1).name

        assertEquals("Johnnie", updatedName)
    }

    @Test
    fun insertTest() {
        val c = Customer(3, "Jane")

        db.execute {
            "INSERT INTO customers VALUES (${p(c.id)}, ${p(c.name)})"
        }

        val jane = getCustomerById(3)

        assertEquals("Customer(id=3, name=Jane)", jane.toString())
    }

    @Test(expected = SQLException::class)
    fun deleteTest() {
        val id = 1
        db.update {
            "DELETE FROM customers WHERE id = ${p(id)}"
        }
        getCustomerById(1)
    }

    @Test
    fun resultSetTest() {
        db.query {
            "SELECT * FROM customers"
        } execute {
            while (resultSet.next()) println(resultSet.getString("name"))
        }
    }

    @Test
    fun sequenceTest() {
        val seq = db.query { "SELECT * FROM customers" } sequence { Customer(this) }
        seq.forEach {
            println("Found customer in seq: $it")
        }
    }

}