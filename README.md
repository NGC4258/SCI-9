# SCI-9

#### This is a crawler to crawling public data from Facebook  
#### And this code will not be updated in the future  
#### Notice of this crawler is used multiprocess to get data  
#### So please take care of your operation, not my fault  

## This code is very ugly, I give suggestion to "Do Not Use"

### How to use ? 
1. Prepare the OS enviroment, last test was in Ubuntu Xenial  
> This is common sense, you can choose which one that you love 

2. Prepare your account of Facebook development to use GraphAPI as following:  
https://developers.facebook.com/  

3. Prepare stored place, only support PostgreSQL
  https://www.postgresql.org/download/linux/ubuntu/  

4. Install packages, the example is Ubuntu Xenial as following:  
> sudo apt install git python-pip libffi-dev libssl-dev

5. Install python libraries as following:  
> sudo pip install python-dateutil psycopg2 'requests[security]' elasticsearch  

6. Download SCI-9 to your machine:  
> git clone https://github.com/NGC4258/SCI-9.git  

7. Change ini file to ingratiate your environment:  
> vi SCI-9/sci-9.ini  

8. Insert group ID what you want to fetching:  
> vi SCI-9/groups  

9. Enjoy!  
> python SCI-9  
