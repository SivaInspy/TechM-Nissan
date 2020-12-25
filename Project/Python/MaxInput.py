# creating an empty list
my_dict = {}
sec_dict = {}
# number of test cases as input
t = int(input("Enter number of test cases : "))
for i1 in range(0, t):
    lst = []
    str_list = []
    l1 = []
    # number of tweets as input
    n = int(input("Enter number of tweets : "))
    # iterating till the range
    for i2 in range(0, n):
        str_list = list(map(str, input().split()))
        lst.append(str_list) # adding the element
    for items in lst:
        for item in items:
            if item.isalpha():
                l1.append(item)
    my_dict = {i:l1.count(i) for i in l1}
    print(my_dict)
    sec_dict.update(my_dict)
    print(sec_dict)
print("Output:")
for x in sec_dict.keys():
    if sec_dict[x] != 1:
        print(x +" => " + str(sec_dict[x]))
