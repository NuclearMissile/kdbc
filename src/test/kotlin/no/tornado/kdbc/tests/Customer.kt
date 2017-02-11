package no.tornado.kdbc.tests

import kdbc.*

data class Customer(var id: Int? = null, var name: String) {
    constructor(t: CustomerTable) : this(t.ID(), t.NAME())
}

class CustomerTable : Table("customer") {
    val ID by column("integer not null primary key auto_increment", INTEGER)
    val NAME by column("text", TEXT_NOT_NULL)
}

class InsertCustomer(customer: Customer) : Insert() {
    val C = CustomerTable()

    init {
        INSERT(C) {
            C.NAME `=` customer.name
        }
        generatedKeys {
            customer.id = getInt(1)
        }
    }
}

class InsertCustomersInBatch(customers: List<Customer>) : Insert() {
    val C = CustomerTable()

    init {
        // H2 Does not support generated keys in batch, so we can't retrieve them with `generatedKeys { }` here
        BATCH(customers) { customer ->
            INSERT(C) {
                C.NAME `=` customer.name
            }
        }
    }
}

class SelectCustomer : Query<Customer>() {
    val C = CustomerTable()

    init {
        SELECT(C)
        FROM(C)
        TO { Customer(C) }
    }

    fun byId(id: Int?): Customer? = let {
        WHERE { C.ID `=` id }
        firstOrNull()
    }
}

class UpdateCustomer(customer: Customer) : Update() {
    val C = CustomerTable()

    init {
        UPDATE(C) {
            C.NAME `=` customer.name
        }
        WHERE {
            C.ID `=` customer.id
        }
    }
}

fun updateCustomer(customer: Customer) = CustomerTable()
        .UPDATE({ it.NAME `=` customer.name }, { it.ID `=` customer.id })

class DeleteCustomer(id: Int) : Delete() {
    val C = CustomerTable()

    init {
        DELETE(C) {
            C.ID `=` id
        }
    }
}

fun deleteCustomer(id: Int) = CustomerTable().DELETE { it.ID `=` id }
