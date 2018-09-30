//Region
CREATE (region1: Region {name: "Europe"})
CREATE (region2: Region {name: "America"})

//Customer
CREATE (cust1: Customer {mktsegment: "mktsegment1"})
CREATE (cust2: Customer {mktsegment: "mktsegment2"})
CREATE (cust3: Customer {mktsegment: "mktsegment3"})

//Order
CREATE (order1: Order {shippriority: 1})
CREATE (order2: Order {shippriority: 2})
CREATE (order3: Order {shippriority: 3})

//Size
CREATE (size1: Size {size: 11})
CREATE (size2: Size {size: 7})

//Supplier
CREATE (supp1: Supplier {acctbal: 10.0, name: "name1", address: "address1", phone: 123456789, comment: "comment1"})
CREATE (supp2: Supplier {acctbal: 20.0, name: "name2", address: "address2", phone: 135792468, comment: "comment2"})
CREATE (supp3: Supplier {acctbal: 30.0, name: "name3", address: "address3", phone: 246813579, comment: "comment3"})

//Part
CREATE (part1: Part {partkey: 1, mfgr: "mfgr1", type: "type1"})
CREATE (part2: Part {partkey: 2, mfgr: "mfgr2", type: "type2"})
CREATE (part3: Part {partkey: 3, mfgr: "mfgr3", type: "type1"})

//Lineitem order1
CREATE (lineitem1: Lineitem {returnflag: "1", linestatus: "1", quantity: 1, extendedprice: 3, discount: 0.1, tax: 0.3, orderkey: 1, shipdate: "date1"})
CREATE (lineitem2: Lineitem {returnflag: "1", linestatus: "1", quantity: 2, extendedprice: 4, discount: 0.2, tax: 0.2, orderkey: 1, shipdate: "date1"})
CREATE (lineitem3: Lineitem {returnflag: "2", linestatus: "1", quantity: 5, extendedprice: 5, discount: 0.1, tax: 0.3, orderkey: 1, shipdate: "date1"})
CREATE (lineitem4: Lineitem {returnflag: "1", linestatus: "1", quantity: 1, extendedprice: 3, discount: 0.2, tax: 0.3, orderkey: 1, shipdate: "date1"})

//Lineitem order2
CREATE (lineitem5: Lineitem {returnflag: "2", linestatus: "2", quantity: 7, extendedprice: 4, discount: 0.3, tax: 0.3, orderkey: 2, shipdate: "date2"})
CREATE (lineitem6: Lineitem {returnflag: "3", linestatus: "3", quantity: 3, extendedprice: 3, discount: 0.3, tax: 0.2, orderkey: 2, shipdate: "date2"})
CREATE (lineitem7: Lineitem {returnflag: "3", linestatus: "1", quantity: 3, extendedprice: 4, discount: 0.3, tax: 0.2, orderkey: 2, shipdate: "date2"})

//Lineitem order3
CREATE (lineitem8: Lineitem {returnflag: "3", linestatus: "4", quantity: 4, extendedprice: 2, discount: 0.3, tax: 0.3, orderkey: 3, shipdate: "date3"})
CREATE (lineitem9: Lineitem {returnflag: "3", linestatus: "4", quantity: 2, extendedprice: 3, discount: 0.3, tax: 0.3, orderkey: 3, shipdate: "date3"})
CREATE (lineitem10: Lineitem {returnflag: "3", linestatus: "3", quantity: 3, extendedprice: 5, discount: 0.5, tax: 0.3, orderkey: 3, shipdate: "date3"})
CREATE (lineitem11: Lineitem {returnflag: "3", linestatus: "3", quantity: 1, extendedprice: 5, discount: 0.5, tax: 0.3, orderkey: 3, shipdate: "date3"})


//Relación customers -> region
CREATE (region1)-[:From { nation_name: "Espanya"}]->(cust1)
CREATE (region1)-[:From { nation_name: "Portugal"}]->(cust2)
CREATE (region2)-[:From { nation_name: "EEUU"}]->(cust3)

//Relación customers -> orders
CREATE (cust1)-[:CustomerOrder {orderdate: "date10"}]->(order1)
CREATE (cust2)-[:CustomerOrder {orderdate: "date11"}]->(order2)
CREATE (cust3)-[:CustomerOrder {orderdate: "date11"}]->(order3)

//Relación region -> suppliers
CREATE (region1)-[:SupplierFrom {nation_name: "Espanya"}]->(supp1)
CREATE (region1)-[:SupplierFrom {nation_name: "Portugal"}]->(supp2)
CREATE (region2)-[:SupplierFrom {nation_name: "EEUU"}]->(supp3)

//Relación part -> supply
CREATE (part1)-[:PartSupplier {supplycost: 10}]->(supp1)
CREATE (part1)-[:PartSupplier {supplycost: 20}]->(supp2)
CREATE (part2)-[:PartSupplier {supplycost: 20}]->(supp2)
CREATE (part2)-[:PartSupplier {supplycost: 30}]->(supp3)

//Relación size -> parts
CREATE (size1)-[:PartSize]->(part1)
CREATE (size2)-[:PartSize]->(part2)

//Relaciones order --> lineitem
CREATE (order1)-[:OrderLineitem { shipdate: "date1"}]->(lineitem1)
CREATE (order2)-[:OrderLineitem { shipdate: "date2"}]->(lineitem5)
CREATE (order3)-[:OrderLineitem { shipdate: "date3"}]->(lineitem8)

// Relacions lineitem-supplier

CREATE (lineitem1)-[:LineitemSupplier]->(supp1)
CREATE (lineitem5)-[:LineitemSupplier]->(supp1)
CREATE (lineitem1)-[:LineitemSupplier]->(supp2)
CREATE (lineitem8)-[:LineitemSupplier]->(supp2)
CREATE (lineitem5)-[:LineitemSupplier]->(supp3)
CREATE (liineitem8)-[:LineitemSupplier]->(supp3)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//Q1
MATCH (l: Lineitem)
WHERE l.shipdate <= "date2"
RETURN l.returnflag AS returnflag, l.linestatus AS linestatus,
SUM(l.quantity) AS sum_qty,
SUM(l.extendedprice) AS sum_base_price,
SUM(l.extendedprice*(1-l.discount)) AS sum_disc_price,
SUM(l.extendedprice*(1-l.discount)*(1+l.tax)) AS sum_charge,
AVG(l.quantity) AS avg_qty,
AVG(l.extendedprice) AS avg_price,
AVG(l.discount) AS avg_disc,
COUNT(*)  AS count_order
ORDER BY returnflag ASC, linestatus ASC


//Q2
MATCH
(size: Size {size: 11})
WITH
size
MATCH 
(size)-[rel1:PartSize]->(p: Part)-[rel2:PartSupplier]->(s: Supplier)<-[rel3:SupplierFrom]-(r: Region)
MATCH
(p2: Part{partkey: p.partkey})-[rel4:PartSupplier]->(s2: Supplier)<-[rel5:SupplierFrom]-(r2: Region)
WHERE
r2.name = "Europe"
WITH
p, s, r, rel2, rel3,
MIN(rel4.supplycost) AS min_supplycost
WHERE
p.type =~ ".*1" AND
r.name = "Europe" AND
rel2.supplycost = min_supplycost
RETURN
s.acctbal AS s_acctbal,
s.name AS s_name,
rel3.nation_name AS n_name,
p.partkey AS p_partkey,
p.mfgr AS p_mfgr,
s.address AS s_adress,
s.phone AS s_phone,
s.comment AS s_comment
ORDER BY
s.acctbal DESC,
rel3.name ASC,
s.name ASC,
p.partkey ASC


//Q3
MATCH (c: Customer)-[relacio1:CustomerOrder]->(o:Order)-[relacio2:OrderLineitem]->(l:Lineitem)
WHERE c.mktsegment = "mktsegment2" and relacio1.orderdate < "date9" and relacio2.shipdate < "date3"
RETURN l.orderkey, SUM(l.extendedprice*(1-l.discount)) AS revenue, relacio1.orderdate, o.shippriority
ORDER BY revenue DESC, relacio1.orderdate ASC

//Q4
MATCH (r:Region)-[relacio1:From]->(c:Customer)-[relacio2:CustomerOrder]->(o:Order)-[relacio3:OrderLineitem]->(l:Lineitem)-[relacio4:LineitemSupplier]->(s:Supplier)<-[relacio5:SupplierFrom]-()
WHERE r.name = "Europe" and relacio2.orderdate < "date9" and relacio2.orderdate >= "date1" and relacio1.nation_name = relacio5.nation_name
RETURN relacio1.nation_name, SUM(l.extendedprice*(1-l.discount)) AS revenue
ORDER BY revenue DESC
