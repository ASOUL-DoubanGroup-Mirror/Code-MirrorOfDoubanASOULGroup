def getCookieDict(cookiestr: str):
    return {
        ele.lstrip().replace('"', '').split('=')[0]: ele.lstrip().replace('"', '').split('=')[1] for ele in
        cookiestr.split(';')
    }
