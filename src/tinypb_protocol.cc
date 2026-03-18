/*
 * @Author: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @Date: 2026-03-18 16:23:02
 * @LastEditors: 来自火星的码农 15122322+heyzhi@user.noreply.gitee.com
 * @LastEditTime: 2026-03-18 16:24:09
 * @FilePath: /MCoroRpc/src/tinypb_protocol.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "../include/coro.hpp"

namespace Coro {
    char TinyPBProtocol::PB_START = 0x02;
    char TinyPBProtocol::PB_END = 0x03;
}