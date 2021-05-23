import urllib.request, urllib.error, urllib.parse
import json
import mysql.connector
import datetime

def connect_db():
    # fill this out with your db connection info
    connection = mysql.connector.connect(user='youruser', password='yourpwd', host='yourhost', database='yourdb', use_unicode=True)
    return connection

def create_page_url(company, APP_ID, APP_SECRET):
    # create authenticated post URL
    page_args = "/?key=value&access_token=" + APP_ID + "|" + APP_SECRET
    page_url = company + page_args

    return page_url

def create_post_url(graph_url, APP_ID, APP_SECRET):
    # create authenticated post URL
    post_args = "/posts/?key=value&access_token=" + APP_ID + "|" + APP_SECRET
    post_url = graph_url + post_args

    return post_url


def render_to_json(graph_url):
    # render graph url call to JSON
    web_response = urllib.request.urlopen(graph_url)
    readable_page = web_response.read()
    # json_data = json.loads(readable_page)
    json_data = json.loads(readable_page.decode('utf-8'))

    return json_data


def scrape_posts_by_date(graph_url, date, post_data, APP_ID, APP_SECRET):
    # render URL to JSON
    page_posts = render_to_json(graph_url)

    # extract next page
    # To get the link for next page
    if "paging" in list(page_posts.keys()):
        if "next" in list(page_posts["paging"].keys()):
            # Setting the next page url
            next_page = page_posts["paging"]["next"]
            print(next_page)
        else:
            next_page = ""
    else:
        next_page = ""

    # grab all posts
    page_posts = page_posts["data"]

    # boolean to tell us when to stop collecting
    collecting = True

    # for each post capture data
    for post in page_posts:
        # likes_count = get_likes_count(page_posts["id"], APP_ID, APP_SECRET)
        likes_count = ""
        if "message" in list(post.keys()):
            message = post["message"]
        else:
            message = ""

        if "story" in list(post.keys()):
            story = post["story"]
        else:
            story = ""

        creator_data = get_posts_creator(post["id"], APP_ID, APP_SECRET)
        current_post = [post["id"], message, story, likes_count, post["created_time"],creator_data[0],creator_data[1]]
        print(("post create time " + str(current_post[4])))

        if current_post[1] != "error":
            if date <= current_post[4]:
                post_data.append(current_post)


            elif date > current_post[4]:
                print("No more new data")
                collecting = False
                break


    # If we still don't meet date requirements, run on next page
    if collecting == True:
        if next_page != "":
            scrape_posts_by_date(next_page, date, post_data, APP_ID, APP_SECRET)
    return post_data

def get_posts_creator(post_id, APP_ID, APP_SECRET):
    # create Graph API Call
    # ? how to get total counts
    graph_url = "https://graph.facebook.com/"
    creator_args = post_id + "/?fields=from&access_token=" + APP_ID + "|" + APP_SECRET
    creator_url = graph_url + creator_args

    try:
        creator_json = render_to_json(creator_url)
        # extract creator information
        creator = [creator_json["from"]["name"],creator_json["from"]["id"]]
    except urllib.error.HTTPError:
        creator = ["error","error"]

    return creator


def get_likes_count(post_id, APP_ID, APP_SECRET):
    # create Graph API Call
    # ? how to get total counts
    graph_url = "https://graph.facebook.com/"
    likes_args = post_id + "/?fields=likes&summary=true&access_token=" + APP_ID + "|" + APP_SECRET
    likes_url = graph_url + likes_args
    likes_json = render_to_json(likes_url)

    # pick out the likes count
    count_likes = likes_json["summary"]["total_count"]

    return count_likes


def create_comments_url(graph_url, post_id, APP_ID, APP_SECRET):
    # create Graph API Call
    comments_args = str(post_id) + "/comments/?key=value&access_token=" + APP_ID + "|" + APP_SECRET
    comments_url = graph_url + comments_args
    # print comments_url
    return comments_url


def get_comments_data(comments_url, comment_data, post_id):
    # render URL to JSON
    try:
        comments = render_to_json(comments_url)["data"]
    except urllib.error.HTTPError:
        return None
        
    
    # for each comment capture data
    for comment in comments:
        current_comments = []
        current_comments = [comment["id"], comment["message"], comment["from"]["name"],
                            comment["from"]["id"],comment["created_time"], post_id]
        print(current_comments)
        print(("comments from " + current_comments[2]))
        comment_data.append(current_comments)

    # check if there is another page
    try:
        # extract next page
        next_page = comments["paging"]["next"]
    except Exception:
        next_page = None


    # if we have another page, recurse
    if next_page is not None:
        get_comments_data(next_page, comment_data, post_id)
    else:
        return comment_data

def create_likes_url(graph_url, post_id, APP_ID, APP_SECRET):
    # create Graph API Call
    likes_args = post_id + "/likes/?key=value&access_token=" + APP_ID + "|" + APP_SECRET
    likes_url = graph_url + likes_args
    # print likes_url
    return likes_url

def get_likes_data(likes_url, like_data, post_id):
    # render URL to JSON
    try:
        likes = render_to_json(likes_url)["data"]
    except urllib.error.HTTPError:
        return None

    # for each comment capture data
    for like in likes:

        # current_likes = [like["id"].encode('utf-8'),like["name"].encode('utf-8'), post_id]
        # the json file does not provide names
        current_likes = [like["id"], post_id]
        print(("liking from " + str(current_likes[0])))
        like_data.append(current_likes)

    # check if there is another page
    try:
        # extract next page
        next_page = likes["paging"]["next"]
    except Exception:
        next_page = None


    # if we have another page, recurse
    if next_page is not None:
        get_likes_data(next_page, like_data, post_id)
    else:
        return like_data


def main():
    # simple data pull App Secret and App ID
    APP_SECRET = "your APP_SECRET"
    APP_ID = "your APP_ID"

    # to find go to page's FB page, at the end of URL find username
    # e.g. http://facebook.com/walmart, walmart is the username
    list_companies = ["McDonaldsUS", "Starbucks", "applebees", "Macys", "target", "lowes"]
    graph_url = "https://graph.facebook.com/"


    # the time of last crawl: we go back 4000 days to crawl everything from the beginning. Facebook was founded in 2007
    nowtimestamp = datetime.datetime.now()
    last_crawl = datetime.datetime.now() - datetime.timedelta(days=120)  #since Jan-1-2014  #400 each time
    last_crawl = last_crawl.isoformat()

    # create db connection
    connection = connect_db()
    cursor = connection.cursor()

    # SQL statement for adding Facebook page data to database
    insert_info = ("INSERT INTO page_info "
                   "(fb_id, name, time_collected)"
                   "VALUES (%s, %s, %s)")

    insert_posts = ("INSERT INTO post_info "
                    "(fb_post_id, message, story, likes_count, time_created, from_name, from_id, post_type, page_id)"
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")

    # SQL statement for adding the likers list of current post
    insert_likes =  ("INSERT INTO like_info "
                   "(fb_liker_id, liked_id, like_type, page_id)"
                   "VALUES (%s, %s, %s, %s)")

    # SQL statement for adding comment data
    insert_comments = ("INSERT INTO comment_info "
                       "(fb_comment_id, message, from_name, from_id, time_created, post_id, page_id)"
                       "VALUES (%s, %s, %s, %s, %s, %s, %s)")

    # SQL statement for adding reply data
    insert_replys = ("INSERT INTO reply_info "
                       "(fb_reply_id, message, from_name, from_id, time_created, comment_id, page_id)"
                       "VALUES (%s, %s, %s, %s, %s, %s, %s)")

    for company in list_companies:
        # make graph api url with company username
        current_page = graph_url + company
        page_url = create_page_url(current_page, APP_ID, APP_SECRET)
        print(("########### " + company + "  ##############"))
        # open public page in facebook graph api
        json_fbpage = render_to_json(page_url)


        # gather our page level JSON Data
        page_id = json_fbpage["id"]
        page_data = [page_id,json_fbpage["name"]]


        # extract post data
        post_url = create_post_url(current_page, APP_ID, APP_SECRET)
        print(post_url)

        post_data = []
        post_data = scrape_posts_by_date(post_url, last_crawl, post_data, APP_ID, APP_SECRET)


        # insert the data we pulled into db
        page_data.append(nowtimestamp)
        cursor.execute(insert_info, page_data)

        # grab primary key
        last_key = cursor.lastrowid


        # loop through and insert data
        for post in post_data:
            if post[6] == page_id: # if the post creator is the company
                post.append(1) # assign post_type = 1 for company posts; post_type = 2 for posts
            else: 
                post.append(2)
            
            post.append(last_key)
            cursor.execute(insert_posts, post)

            # capture post id of data just inserted
            post_key = cursor.lastrowid
            print("this is the key " + str(post_key))

            # insert liker list
            like_data = []
            likes_url = create_likes_url(graph_url, post[0], APP_ID, APP_SECRET)
            print("like url " + likes_url)
            likes = get_likes_data(likes_url, like_data, post_key)

            if likes is not None:
                for like in likes:
                    like.append(1) # like of post is type 1 liking
                    like.append(last_key)
                    cursor.execute(insert_likes,like)

            # insert comments
            comment_data = []
            comment_url = create_comments_url(graph_url, post[0], APP_ID, APP_SECRET)
            comments = get_comments_data(comment_url, comment_data, post_key)

            if comments is not None:
                for comment in comments:
                    comment.append(last_key)
                    cursor.execute(insert_comments, comment)
    
                    # capture comment id of data just inserted
                    comment_key = cursor.lastrowid
                    
                    # capture comment likes
                    commentlike_data = []
                    comment_like_url = create_likes_url(graph_url, comment[0], APP_ID, APP_SECRET) 
                    commentlikes = get_likes_data(comment_like_url, commentlike_data, comment_key)
    
                    # insert replys
                    if commentlikes is not None:
                        for commentlike in commentlikes:
                            commentlike.append(2)  # like of comments is type 2 liking
                            commentlike.append(last_key)
                            cursor.execute(insert_likes,commentlike)              
                    
                    # capture replies
                    print("get replies for comment " + str(comment[0]))
                    reply_url = create_comments_url(graph_url, comment[0], APP_ID, APP_SECRET)
                    print("url for reply " + reply_url)
                    reply_data = []
                    replys = get_comments_data(reply_url, reply_data, comment_key)
    
                    # insert replys
                    if replys is not None:
                        for reply in replys:
                            reply.append(last_key)
                            cursor.execute(insert_replys, reply)

        # commit the data to the db
        connection.commit()

    connection.close()


if __name__ == "__main__":
    main()
