from Financial_data_crawler.db import clients
import twisted

class tags_extract:
    def __init__(self):
        self.comp_profile= clients.get_mongodb_news_conn()['Comp'].Profile

    def update_tags(self,tags:list[str],date:str):
        '''
        Update companies profile's tags if the tag does not exist
        '''

        #Bad tags
        if '趨勢分析' in tags: return None

        pure_tags=[]
        comp=[]
        advisor=[]

        #Check each tag is a company name or tag name
        for tag in tags:
            if self.comp_profile.find_one({'Name':tag}) is None:
                if '投顧' in tag: advisor.append(tag)
                else:
                    pure_tags.append(tag)
            else:
                comp.append(tag)
        print(pure_tags,comp)
        #Update the company's profile if the tag doesn't exist
        for company in comp:

            # Update advisor and time, only keep tracking the recent 20 records
            # The tags with financial advisor make the accuracy decreased
            if len(advisor)>0:
                self.comp_profile.update_one({'Name': company},
                                             {'$push':{'Advisor':{'$each':advisor,'$slice':-20}},
                                              '$push': {'Advised Time': {'$each': [date],'$slice': -20}}
                                              })

            #Update tags and remove duplicates
            else:
                self.comp_profile.update_one({'Name': company},{'$addToSet':{'Tags': {'$each':pure_tags}}})






