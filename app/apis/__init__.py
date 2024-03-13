from .. import initLog, jobCfg, log
import myConfig as mc
import myCryptor as crypt
from .. import Header, Optional, HTTPException, status
import time

cfg = mc.MyConfig(jobCfg['api-cfg']).config


def authentication(x_service_token: Optional[str] = Header(None),
                   x_sys: Optional[str] = Header(None)):
    key = cfg['auth'][x_sys]['key']
    iv = cfg['auth'][x_sys]['iv']
    plain_text = crypt.aesDecrypt(x_service_token, key, iv)
    for s in plain_text.split(','):
        values = s.split(':')
        if values[0] == 'sys':
            if values[1] != x_sys:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unknown request system!")
        if values[0] == 'currentTime':
            if time.time() - float(values[1]) > 60:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Extend maximum safe time gap!")
    return 'OK'
