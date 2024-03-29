
# B. avg
# Given a list of numbers, return the average of the list.
# If numbers equal [10, 10, 15, 7], the return value should be 10.5
def avg(numbers):
  average = sum(numbers) / len(numbers)
  return average


# Given a string s, return a string made of the first 2
# and the last 2 chars of the original string,
# so 'spring' yields 'spng'. However, if the string length
# is less than 2, return instead the empty string.
def both_ends(s):
  finalString = s[0:2] + s[-2:]
  return finalString

# D. MixUp
# Given strings a and b, return a single string with a and b separated
# by a space '<a> <b>', except swap the first 2 chars of each string.
# e.g.
#   'mix', pod' -> 'pox mid'
#   'dog', 'dinner' -> 'dig donner'
# Assume a and b are length 2 or more.
def mix_up(a, b):
  finalString = b[:2]+a[2:] + " " + a[:2]+b[2:]
  return finalString

# B. uniq
# Given a list of objects (numbers, strings, booleans), return the same list without duplicates.
# If a = [1, True, "otter", "platypus", 1, 2, False, "otter"]
# the return value should be [1, True, "otter", "platypus", 2, False]
# Note that the order does not matter
def uniq(a):
  a1 = []
  for item in a:
    if (item, type(item)) not in [(x,type(x)) for x in a1]:
      a1.append(item)
  return a1

# B. merge
# Given two sorted list of numbers, return a sorted list with the elements of both lists.
# If a = [1, 5, 13, 14] and b = [2, 7, 8, 42]
# the return value should be [1, 2, 5, 7, 8, 13, 14, 42]
def merge(a, b):
  finalList = []
  i = 0
  j = 0
  while i < len(a) and j < len(b):
      if a[i] < b[j]:
        finalList.append(a[i])
        i+=1
      else:
        finalList.append(b[j])
        j += 1
  finalList.extend(a[i:])
  finalList.extend(b[j:])
  return finalList

def main():
  numbers = [10, 10, 15, 7]
  print(avg(numbers))
  print(both_ends("spring"))
  print(mix_up("mix", "pod"))
  print(uniq([1, True, "otter", "platypus", 1, 2, False, "otter"]))
  a = [1, 5, 13, 14]
  b = [2, 7, 8, 42]
  print(merge(a, b))

if __name__ == '__main__':
  main()