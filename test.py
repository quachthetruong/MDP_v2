### for loop a->z
count=0
for i in range(97, 123):
    for j in range(97, 123):
        for k in range(97, 123):
            if chr(i)==chr(j) and chr(j)==chr(k):
                print(chr(i)+chr(j)+chr(k), count)
                count=0
            else:
                count+=1