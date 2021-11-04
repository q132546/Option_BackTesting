def mdd(p_l_list):
    max_point = -1000
    max_dd = 0

    for i in range(len(p_l_list)):
        if p_l_list[i] > max_point:
            max_point = p_l_list[i]

        else:
            if max_point - p_l_list[i] > max_dd:
                max_dd = max_point - p_l_list[i]

    return p_l_list[-1], max_dd


def evaluated_indicators(pnl_np):
    # annual return, annual return / mdd, sharp ratio, rough return

    rough_return = pnl_np[-1] - pnl_np[0]
    rough_return_rate = rough_return / pnl_np[0] * 100

    annual_return = ((1 + rough_return_rate / 100) ** (1 / 2.3) - 1) * 100

    max_drawdown = mdd(pnl_np)[1]
    max_drawdown_rate = mdd(pnl_np)[1] / pnl_np[0] * 100
    sharp = sharp_ratio(pnl_np)

    print('rough return', rough_return, 'annual rough return', rough_return / 2.3, 'rough return rate', rough_return_rate)
    print('annual return rate', annual_return)
    print('mdd', max_drawdown, 'max drawdown rate', max_drawdown_rate)
    print('annual return / mdd', annual_return / max_drawdown_rate)
    print('sharp ratio', sharp)


def sharp_ratio(p_l_list):
    p_l_list = np.array(p_l_list)

    R = pd.DataFrame(p_l_list)
    r = R.diff()

    sr = r.mean() / r.std() * np.sqrt(252)

    return sr.iloc[0]
