class Solution:
    # @return an integer
    def lengthOfLongestSubstring(self, s):
        start = maxLength = 0
        usedChar = {}

        for i in range(len(s)):
            if s[i] in usedChar and start <= usedChar[s[i]]:
                start = usedChar[s[i]] + 1
            else:
                maxLength = max(maxLength, i - start + 1)

            usedChar[s[i]] = i
        print(usedChar)

        return maxLength

if __name__ == '__main__':
    r= Solution.lengthOfLongestSubstring(self='abc',s='abcabc')
    print(r)
