#factory method
#three types of people in the company umbrella ---buyer, seller, and employees

processors= {'customer':customerProcessor,
    'seller':sP,
    'employee':eP
}

rows = [
    {'key':'seller','value':{'name':'Anu Garg','item_type':'book','item_name':'SQL Cookbook'}},
    {'key':"customer","value":{"name":"Jess Pang","item_type":"book","item_name":"SQL Cookbook"}},
    {'key':'employee','value':{'name':'Carly Taylor','id':1,'account_owned':'Anu Garg'}},
    {'key':'employee','value':{'name':'Carl Loyr','id':1,'account_owned':'Jess Pang'}}

]
for row in rows:
    print(p[row['key']]())
