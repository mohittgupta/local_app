package com.pickmytrade.ibapp.bussinesslogic;

import com.ib.controller.ApiController;

public class IBController {
    private static ApiController controller;
    private static final Object lock = new Object();

    public static ApiController getController() {
        synchronized (lock) {
            if (controller == null) {
                controller = new ApiController(new ApiController.IConnectionHandler() {
                    @Override
                    public void connected() {}

                    @Override
                    public void disconnected() {}

                    @Override
                    public void accountList(java.util.List<String> list) {}

                    @Override
                    public void error(Exception e) {}

                    @Override
                    public void message(int id, int errorCode, String errorMsg, String advancedOrderRejectJson) {}

                    @Override
                    public void show(String string) {}
                });
            }
            return controller;
        }
    }

    public static void setController(ApiController newController) {
        synchronized (lock) {
            controller = newController;
        }
    }
}