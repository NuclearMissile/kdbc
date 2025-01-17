package no.tornado.kdbc.tests

import kdbc.KDBC
import kdbc.transaction
import no.tornado.kdbc.tests.models.Customer
import no.tornado.kdbc.tests.tables.CUSTOMER
import org.h2.jdbcx.JdbcDataSource
import java.sql.SQLException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class QueryTests {
    companion object {
        init {
            val dataSource = JdbcDataSource()
            dataSource.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
            KDBC.setDataSource(dataSource)
            CUSTOMER().create()
            val customers = listOf(Customer(name = "John"), Customer(name = "Jill"))
            InsertCustomersInBatch(customers).execute()
        }
    }

    @Test
    fun queryTest() {
        val john = SelectCustomer().byId(1)!!
        assertEquals(1, john.id)
        assertEquals("John", john.name)
    }

    @Test
    fun updateTest() {
        UpdateCustomer(Customer(1, "Johnnie")).execute()

        val updatedName = SelectCustomer().byId(1)?.name

        assertEquals("Johnnie", updatedName)
    }

    @Test
    fun insertTest() {
        val newCustomer = Customer(name = "Jane")
        InsertCustomer(newCustomer).execute()

        val fromDatabase = SelectCustomer().byId(newCustomer.id!!)!!

        assertEquals(newCustomer.id, fromDatabase.id)
        assertEquals(newCustomer.name, fromDatabase.name)
        assertEquals(newCustomer.uuid, fromDatabase.uuid)
    }

    @Test
    fun deleteTest() {
        DeleteCustomer(1).execute()
        assertNull(SelectCustomer().byId(1))
    }

    @Test
    fun adhocResultSetTest() {
        SelectCustomer().apply {
            execute()
            val rs = resultSet
            while (rs.next()) println(rs.getString("name"))
        }
    }

    @Test
    fun nested_transaction_rollback() {
        try {
            transaction {
                DeleteCustomer(1).execute()
                transaction {
                    DeleteCustomer(2).execute()
                    throw SQLException("I'm naughty")
                }
            }
        } catch (_: SQLException) {
        }
        assertNotNull("Customer 1 should still be available", SelectCustomer().byId(1).toString())
        assertNotNull("Customer 2 should still be available", SelectCustomer().byId(2).toString())
    }

}