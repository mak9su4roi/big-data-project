#! /bin/bash
declare -i res=1
while [ $res -ne 0 ]; do
    sleep 10
    python3 -c "from common.session import session;session('node',9042,'wiki');exit(0)"
    res=${?}
done
echo "START API"
uvicorn api:app --host 0.0.0.0 --port 8080